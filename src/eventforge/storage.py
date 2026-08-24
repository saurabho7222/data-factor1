"""SQLite persistence with transactional ingestion and a durable work queue."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from .errors import SchemaVersionError, StorageUnavailable
from .models import IngestResult
from .schemas import EventIn

SCHEMA_VERSION = 1
Clock = Callable[[], datetime]

_SCHEMA = """
CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    source TEXT NOT NULL,
    event_type TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    received_at TEXT NOT NULL,
    UNIQUE (tenant_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_events_tenant_time
    ON events (tenant_id, occurred_at, event_type);
CREATE TABLE IF NOT EXISTS jobs (
    job_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL REFERENCES events(event_id),
    tenant_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('project', 'replay')),
    status TEXT NOT NULL CHECK (status IN ('pending', 'processing', 'done', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at TEXT NOT NULL,
    last_error TEXT,
    unique_key TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_claim
    ON jobs (status, available_at, job_id);
CREATE TABLE IF NOT EXISTS projections (
    event_id TEXT PRIMARY KEY REFERENCES events(event_id),
    tenant_id TEXT NOT NULL,
    source TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_date TEXT NOT NULL,
    payload_bytes INTEGER NOT NULL CHECK (payload_bytes >= 0),
    projected_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_projections_analytics
    ON projections (tenant_id, event_date, event_type);
"""


def utc_now() -> datetime:
    return datetime.now(UTC)


class Database:
    """Own SQLite connection policy, clock policy, and transactional operations."""

    def __init__(self, path: Path, *, clock: Clock = utc_now) -> None:
        self.path = path
        self._clock = clock

    def now(self) -> datetime:
        """Return the authoritative timezone-aware application time."""
        current = self._clock()
        if current.tzinfo is None or current.utcoffset() is None:
            raise ValueError("database clock must return a timezone-aware datetime")
        return current

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            connection = sqlite3.connect(self.path, timeout=5.0)
        except sqlite3.Error as exc:
            raise StorageUnavailable("unable to open event store") from exc
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA journal_mode = WAL")
        try:
            yield connection
        finally:
            connection.close()

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(_SCHEMA)
            row = connection.execute("SELECT value FROM metadata WHERE key = 'schema_version'").fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO metadata(key, value) VALUES('schema_version', ?)",
                    (str(SCHEMA_VERSION),),
                )
                connection.commit()
                return
            if int(row["value"]) != SCHEMA_VERSION:
                raise SchemaVersionError(
                    f"database schema version {row['value']} is incompatible with {SCHEMA_VERSION}"
                )

    def healthcheck(self) -> bool:
        with self.connect() as connection:
            result = connection.execute("PRAGMA quick_check").fetchone()
            version = connection.execute("SELECT value FROM metadata WHERE key = 'schema_version'").fetchone()
        return result is not None and result[0] == "ok" and version is not None and int(version["value"]) == SCHEMA_VERSION

    @staticmethod
    def _insert_event(connection: sqlite3.Connection, event: EventIn, *, now: str) -> IngestResult:
        event_id = str(uuid4())
        payload_json = json.dumps(event.payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        try:
            connection.execute(
                """
                INSERT INTO events(
                    event_id, tenant_id, source, event_type, occurred_at,
                    payload_json, idempotency_key, received_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    event.tenant_id,
                    event.source,
                    event.event_type,
                    event.occurred_at.isoformat(),
                    payload_json,
                    event.idempotency_key,
                    now,
                ),
            )
        except sqlite3.IntegrityError:
            existing = connection.execute(
                "SELECT event_id FROM events WHERE tenant_id = ? AND idempotency_key = ?",
                (event.tenant_id, event.idempotency_key),
            ).fetchone()
            if existing is None:
                raise
            return IngestResult(event_id=str(existing["event_id"]), duplicate=True, queued=False)

        connection.execute(
            """
            INSERT INTO jobs(
                event_id, tenant_id, kind, status, attempts, available_at,
                unique_key, created_at, updated_at
            ) VALUES (?, ?, 'project', 'pending', 0, ?, ?, ?, ?)
            """,
            (event_id, event.tenant_id, now, f"project:{event_id}", now, now),
        )
        return IngestResult(event_id=event_id, duplicate=False, queued=True)

    def ingest_batch(self, events: Sequence[EventIn]) -> list[IngestResult]:
        """Ingest a bounded validated batch in one transaction, preserving input order."""
        if not events:
            raise ValueError("ingest_batch requires at least one event")
        now = self.now().isoformat()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                results = [self._insert_event(connection, event, now=now) for event in events]
            except Exception:
                connection.rollback()
                raise
            connection.commit()
        return results

    def ingest(self, event: EventIn) -> IngestResult:
        return self.ingest_batch([event])[0]

    def counts(self) -> dict[str, int]:
        with self.connect() as connection:
            return {
                "events": int(connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]),
                "jobs": int(connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]),
                "projections": int(connection.execute("SELECT COUNT(*) FROM projections").fetchone()[0]),
            }
