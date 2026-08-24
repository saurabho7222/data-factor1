from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from eventforge.models import JobRecord
from eventforge.processing import Worker
from eventforge.replay import enqueue_replay
from eventforge.schemas import EventIn, ReplayRequest
from eventforge.storage import Database

NOW = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)


def database_at(tmp_path: Path) -> Database:
    return Database(tmp_path / "events.db", clock=lambda: NOW)


def event(key: str, *, event_type: str = "order.completed") -> EventIn:
    return EventIn(
        tenant_id="acme",
        source="checkout.api",
        event_type=event_type,
        idempotency_key=key,
        occurred_at=NOW,
        payload={"key": key},
    )


def test_worker_claims_projects_and_completes_job(tmp_path: Path) -> None:
    database = database_at(tmp_path)
    database.initialize()
    database.ingest(event("event-key-0001"))
    assert Worker(database).process_one(now=NOW + timedelta(seconds=1)) is True
    assert database.counts()["projections"] == 1
    with database.connect() as connection:
        job = connection.execute("SELECT status, attempts FROM jobs").fetchone()
    assert tuple(job) == ("done", 1)


def test_replay_is_idempotent_and_does_not_duplicate_projection(tmp_path: Path) -> None:
    database = database_at(tmp_path)
    database.initialize()
    database.ingest(event("event-key-0002"))
    worker = Worker(database)
    worker.process_one(now=NOW + timedelta(seconds=1))
    assert enqueue_replay(database, ReplayRequest(tenant_id="acme")) == 1
    worker.process_one(now=NOW + timedelta(seconds=2))
    assert database.counts()["projections"] == 1
    with database.connect() as connection:
        statuses = [row[0] for row in connection.execute("SELECT status FROM jobs ORDER BY job_id")]
    assert statuses == ["done", "done"]


def test_replay_filters_event_type_and_respects_limit(tmp_path: Path) -> None:
    database = database_at(tmp_path)
    database.initialize()
    database.ingest(event("event-key-0003", event_type="order.completed"))
    database.ingest(event("event-key-0004", event_type="order.refunded"))
    queued = enqueue_replay(database, ReplayRequest(tenant_id="acme", event_type="order.completed", limit=1))
    assert queued == 1


def test_worker_retries_failure_with_backoff_then_marks_terminal_failure(tmp_path: Path) -> None:
    database = database_at(tmp_path)
    database.initialize()
    database.ingest(event("event-key-0005"))

    def failing_projector(_database: Database, _job: JobRecord, _now: datetime) -> None:
        raise RuntimeError("projection failed")

    worker = Worker(database, max_attempts=2, projector=failing_projector)
    assert worker.process_one(now=NOW + timedelta(seconds=1)) is True
    with database.connect() as connection:
        first = connection.execute("SELECT status, attempts, available_at, last_error FROM jobs").fetchone()
    assert first[0] == "pending"
    assert first[1] == 1
    assert first[3] == "RuntimeError"

    retry_at = datetime.fromisoformat(str(first[2]))
    assert retry_at > NOW + timedelta(seconds=1)
    assert worker.process_one(now=retry_at) is True
    with database.connect() as connection:
        final = connection.execute("SELECT status, attempts, last_error FROM jobs").fetchone()
    assert tuple(final) == ("failed", 2, "RuntimeError")


def test_worker_returns_false_when_queue_has_no_claimable_jobs(tmp_path: Path) -> None:
    database = database_at(tmp_path)
    database.initialize()
    assert Worker(database).process_one(now=NOW) is False


def test_ingestion_replay_and_worker_share_authoritative_clock(tmp_path: Path) -> None:
    database = database_at(tmp_path)
    database.initialize()
    database.ingest(event("event-key-clock"))
    with database.connect() as connection:
        created = connection.execute("SELECT available_at, created_at FROM jobs WHERE kind='project'").fetchone()
    assert tuple(created) == (NOW.isoformat(), NOW.isoformat())

    assert Worker(database).process_one() is True
    assert enqueue_replay(database, ReplayRequest(tenant_id="acme")) == 1
    with database.connect() as connection:
        replay = connection.execute(
            "SELECT available_at, created_at FROM jobs WHERE kind='replay' ORDER BY job_id DESC LIMIT 1"
        ).fetchone()
    assert tuple(replay) == (NOW.isoformat(), NOW.isoformat())
