"""Bounded replay scheduling for immutable raw events."""

from __future__ import annotations

from uuid import uuid4

from .schemas import ReplayRequest
from .storage import Database, utc_now


def enqueue_replay(database: Database, request: ReplayRequest) -> int:
    """Queue matching events for idempotent reprojection and return queued count."""
    with database.connect() as connection:
        rows = connection.execute(
            """
            SELECT event_id
            FROM events
            WHERE tenant_id = ?
              AND (? IS NULL OR event_type = ?)
              AND (? IS NULL OR substr(occurred_at, 1, 10) >= ?)
              AND (? IS NULL OR substr(occurred_at, 1, 10) <= ?)
            ORDER BY occurred_at, event_id
            LIMIT ?
            """,
            (
                request.tenant_id,
                request.event_type,
                request.event_type,
                request.start_date.isoformat() if request.start_date else None,
                request.start_date.isoformat() if request.start_date else None,
                request.end_date.isoformat() if request.end_date else None,
                request.end_date.isoformat() if request.end_date else None,
                request.limit,
            ),
        ).fetchall()
        now = utc_now().isoformat()
        connection.execute("BEGIN IMMEDIATE")
        for row in rows:
            connection.execute(
                """
                INSERT INTO jobs(
                    event_id, tenant_id, kind, status, attempts, available_at,
                    unique_key, created_at, updated_at
                ) VALUES (?, ?, 'replay', 'pending', 0, ?, ?, ?, ?)
                """,
                (
                    str(row["event_id"]),
                    request.tenant_id,
                    now,
                    f"replay:{uuid4()}",
                    now,
                    now,
                ),
            )
        connection.commit()
    return len(rows)
