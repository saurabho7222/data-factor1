from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from eventforge.schemas import EventIn
from eventforge.storage import SCHEMA_VERSION, Database


def make_event(*, tenant_id: str = "acme", key: str = "order-2026-0001") -> EventIn:
    return EventIn(
        tenant_id=tenant_id,
        source="checkout.api",
        event_type="order.completed",
        idempotency_key=key,
        occurred_at=datetime(2026, 8, 24, 12, 0, tzinfo=UTC),
        payload={"order_id": "o-1", "amount": 42.5},
    )


def test_initialize_creates_current_schema_and_passes_healthcheck(tmp_path: Path) -> None:
    database = Database(tmp_path / "events.db")
    database.initialize()
    assert database.healthcheck() is True
    with database.connect() as connection:
        version = connection.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()[0]
        foreign_keys = connection.execute("PRAGMA foreign_keys").fetchone()[0]
        busy_timeout = connection.execute("PRAGMA busy_timeout").fetchone()[0]
    assert int(version) == SCHEMA_VERSION
    assert foreign_keys == 1
    assert busy_timeout == 5000


def test_ingest_atomically_queues_exactly_one_projection_job(tmp_path: Path) -> None:
    database = Database(tmp_path / "events.db")
    database.initialize()
    result = database.ingest(make_event())
    assert result.duplicate is False
    assert result.queued is True
    assert database.counts() == {"events": 1, "jobs": 1, "projections": 0}


def test_idempotency_returns_original_event_without_duplicate_job(tmp_path: Path) -> None:
    database = Database(tmp_path / "events.db")
    database.initialize()
    first = database.ingest(make_event())
    duplicate = database.ingest(make_event())
    assert duplicate.event_id == first.event_id
    assert duplicate.duplicate is True
    assert duplicate.queued is False
    assert database.counts() == {"events": 1, "jobs": 1, "projections": 0}


def test_idempotency_key_is_scoped_per_tenant(tmp_path: Path) -> None:
    database = Database(tmp_path / "events.db")
    database.initialize()
    first = database.ingest(make_event(tenant_id="acme"))
    second = database.ingest(make_event(tenant_id="globex"))
    assert first.event_id != second.event_id
    assert database.counts()["events"] == 2


def test_payload_is_canonicalized_before_storage(tmp_path: Path) -> None:
    database = Database(tmp_path / "events.db")
    database.initialize()
    result = database.ingest(make_event())
    with database.connect() as connection:
        payload = connection.execute("SELECT payload_json FROM events WHERE event_id = ?", (result.event_id,)).fetchone()[0]
    assert payload == '{"amount":42.5,"order_id":"o-1"}'
