from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from fastapi.testclient import TestClient

from eventforge.api import create_app
from eventforge.dead_letters import list_dead_letters, retry_dead_letters
from eventforge.models import JobRecord
from eventforge.processing import Worker
from eventforge.schemas import EventIn
from eventforge.storage import Database

NOW = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)


def event(key: str, *, tenant: str = "acme") -> EventIn:
    return EventIn(
        tenant_id=tenant,
        source="deadletter.test",
        event_type="projection.failed",
        idempotency_key=key,
        occurred_at=NOW,
        payload={"key": key},
    )


def fail_projector(_database: Database, _job: JobRecord, _now: datetime) -> None:
    raise RuntimeError("forced projection failure")


def fail_one(database: Database, key: str, *, tenant: str = "acme") -> int:
    result = database.ingest(event(key, tenant=tenant))
    assert Worker(database, max_attempts=1, projector=fail_projector).process_one() is True
    with database.connect() as connection:
        row = connection.execute("SELECT job_id FROM jobs WHERE event_id=?", (result.event_id,)).fetchone()
    return int(row["job_id"])


def test_dead_letter_listing_is_tenant_scoped_and_preserves_failure_context(tmp_path: Path) -> None:
    database = Database(tmp_path / "events.db", clock=lambda: NOW)
    database.initialize()
    acme_job = fail_one(database, "deadletter-acme-01")
    fail_one(database, "deadletter-globex1", tenant="globex")

    rows = list_dead_letters(database, tenant_id="acme")

    assert len(rows) == 1
    assert rows[0].job_id == acme_job
    assert rows[0].tenant_id == "acme"
    assert rows[0].attempts == 1
    assert rows[0].last_error == "RuntimeError"


def test_retry_dead_letters_resets_failure_and_allows_normal_recovery(tmp_path: Path) -> None:
    database = Database(tmp_path / "events.db", clock=lambda: NOW)
    database.initialize()
    fail_one(database, "deadletter-retry01")

    assert retry_dead_letters(database, tenant_id="acme", limit=10) == 1
    with database.connect() as connection:
        state = connection.execute("SELECT status, attempts, last_error, available_at FROM jobs").fetchone()
    assert tuple(state[:3]) == ("pending", 0, None)
    assert state[3] == NOW.isoformat()

    assert Worker(database).process_one() is True
    assert database.counts()["projections"] == 1
    assert list_dead_letters(database, tenant_id="acme") == []


def test_dead_letter_api_lists_and_requeues_failures_with_metrics(tmp_path: Path) -> None:
    path = tmp_path / "events.db"
    database = Database(path)
    database.initialize()
    fail_one(database, "deadletter-api-001")
    client = TestClient(create_app(database_path=path))

    listing = client.get("/v1/dead-letters", params={"tenant_id": "acme", "limit": 10})
    retry = client.post("/v1/dead-letters/retry", json={"tenant_id": "acme", "limit": 10})

    assert listing.status_code == 200
    assert len(listing.json()) == 1
    assert listing.json()[0]["last_error"] == "RuntimeError"
    assert retry.status_code == 202
    assert retry.json() == {"requeued": 1}
    assert "eventforge_dead_letter_jobs_requeued_total 1" in client.get("/metrics").text


def test_dead_letter_api_validates_tenant_and_limit_boundaries(tmp_path: Path) -> None:
    client = TestClient(create_app(database_path=tmp_path / "events.db"))
    assert client.get("/v1/dead-letters", params={"tenant_id": "INVALID TENANT"}).status_code == 422
    assert client.post("/v1/dead-letters/retry", json={"tenant_id": "acme", "limit": 501}).status_code == 422
