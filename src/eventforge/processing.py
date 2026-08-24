"""Retryable at-least-once projection worker over the durable SQLite queue."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from datetime import datetime, timedelta, timezone

from .models import JobRecord
from .storage import Database

Projector = Callable[[Database, JobRecord, datetime], None]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def claim_next(database: Database, *, now: datetime | None = None) -> JobRecord | None:
    current = (now or utc_now()).isoformat()
    with database.connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            """
            SELECT job_id, event_id, tenant_id, kind, attempts
            FROM jobs
            WHERE status = 'pending' AND available_at <= ?
            ORDER BY job_id
            LIMIT 1
            """,
            (current,),
        ).fetchone()
        if row is None:
            connection.rollback()
            return None
        attempts = int(row["attempts"]) + 1
        connection.execute(
            "UPDATE jobs SET status='processing', attempts=?, updated_at=? WHERE job_id=?",
            (attempts, current, int(row["job_id"])),
        )
        connection.commit()
    return JobRecord(
        job_id=int(row["job_id"]),
        event_id=str(row["event_id"]),
        tenant_id=str(row["tenant_id"]),
        kind=str(row["kind"]),
        attempts=attempts,
    )


def project_event(database: Database, job: JobRecord, now: datetime) -> None:
    """Upsert one immutable event into the query projection without double counting."""
    with database.connect() as connection:
        event = connection.execute(
            """
            SELECT event_id, tenant_id, source, event_type, occurred_at, payload_json
            FROM events WHERE event_id=?
            """,
            (job.event_id,),
        ).fetchone()
        if event is None:
            raise RuntimeError(f"event {job.event_id} disappeared before projection")
        payload_bytes = len(str(event["payload_json"]).encode("utf-8"))
        connection.execute(
            """
            INSERT INTO projections(
                event_id, tenant_id, source, event_type, event_date, payload_bytes, projected_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                tenant_id=excluded.tenant_id,
                source=excluded.source,
                event_type=excluded.event_type,
                event_date=excluded.event_date,
                payload_bytes=excluded.payload_bytes,
                projected_at=excluded.projected_at
            """,
            (
                str(event["event_id"]),
                str(event["tenant_id"]),
                str(event["source"]),
                str(event["event_type"]),
                str(event["occurred_at"])[:10],
                payload_bytes,
                now.isoformat(),
            ),
        )
        connection.commit()


def mark_done(database: Database, job_id: int, *, now: datetime) -> None:
    with database.connect() as connection:
        connection.execute(
            "UPDATE jobs SET status='done', last_error=NULL, updated_at=? WHERE job_id=?",
            (now.isoformat(), job_id),
        )
        connection.commit()


def mark_failed_or_retry(
    database: Database,
    job: JobRecord,
    error: Exception,
    *,
    now: datetime,
    max_attempts: int,
) -> None:
    terminal = job.attempts >= max_attempts
    status = "failed" if terminal else "pending"
    delay_seconds = min(300, 2 ** max(0, job.attempts - 1))
    available_at = now if terminal else now + timedelta(seconds=delay_seconds)
    with database.connect() as connection:
        connection.execute(
            """
            UPDATE jobs
            SET status=?, available_at=?, last_error=?, updated_at=?
            WHERE job_id=?
            """,
            (status, available_at.isoformat(), type(error).__name__, now.isoformat(), job.job_id),
        )
        connection.commit()


class Worker:
    """Process durable jobs one at a time with bounded exponential retry."""

    def __init__(
        self,
        database: Database,
        *,
        max_attempts: int = 3,
        projector: Projector = project_event,
    ) -> None:
        if max_attempts < 1 or max_attempts > 20:
            raise ValueError("max_attempts must be between 1 and 20")
        self.database = database
        self.max_attempts = max_attempts
        self.projector = projector

    def process_one(self, *, now: datetime | None = None) -> bool:
        current = now or utc_now()
        job = claim_next(self.database, now=current)
        if job is None:
            return False
        try:
            self.projector(self.database, job, current)
        except (sqlite3.Error, RuntimeError, ValueError) as exc:
            mark_failed_or_retry(
                self.database,
                job,
                exc,
                now=current,
                max_attempts=self.max_attempts,
            )
            return True
        mark_done(self.database, job.job_id, now=current)
        return True
