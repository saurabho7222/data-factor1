"""Tenant-scoped dead-letter inspection and bounded recovery operations."""

from __future__ import annotations

from dataclasses import dataclass

from .storage import Database


@dataclass(frozen=True, slots=True)
class DeadLetterRecord:
    job_id: int
    event_id: str
    tenant_id: str
    kind: str
    attempts: int
    last_error: str | None
    updated_at: str


def list_dead_letters(database: Database, *, tenant_id: str, limit: int = 100) -> list[DeadLetterRecord]:
    """Return the oldest terminal failures for one tenant."""
    if limit < 1 or limit > 500:
        raise ValueError("limit must be between 1 and 500")
    with database.connect() as connection:
        rows = connection.execute(
            """
            SELECT job_id, event_id, tenant_id, kind, attempts, last_error, updated_at
            FROM jobs
            WHERE tenant_id = ? AND status = 'failed'
            ORDER BY updated_at, job_id
            LIMIT ?
            """,
            (tenant_id, limit),
        ).fetchall()
    return [
        DeadLetterRecord(
            job_id=int(row["job_id"]),
            event_id=str(row["event_id"]),
            tenant_id=str(row["tenant_id"]),
            kind=str(row["kind"]),
            attempts=int(row["attempts"]),
            last_error=str(row["last_error"]) if row["last_error"] is not None else None,
            updated_at=str(row["updated_at"]),
        )
        for row in rows
    ]


def retry_dead_letters(database: Database, *, tenant_id: str, limit: int = 100) -> int:
    """Atomically requeue a bounded set of terminal failures for one tenant."""
    if limit < 1 or limit > 500:
        raise ValueError("limit must be between 1 and 500")
    now = database.now().isoformat()
    with database.connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        rows = connection.execute(
            """
            SELECT job_id
            FROM jobs
            WHERE tenant_id = ? AND status = 'failed'
            ORDER BY updated_at, job_id
            LIMIT ?
            """,
            (tenant_id, limit),
        ).fetchall()
        if rows:
            connection.executemany(
                """
                UPDATE jobs
                SET status='pending', attempts=0, available_at=?, last_error=NULL, updated_at=?
                WHERE job_id=? AND tenant_id=? AND status='failed'
                """,
                [(now, now, int(row["job_id"]), tenant_id) for row in rows],
            )
        connection.commit()
    return len(rows)
