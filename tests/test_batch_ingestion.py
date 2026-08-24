from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from eventforge.api import create_app
from eventforge.schemas import EventIn
from eventforge.storage import Database

NOW = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)


def event(key: str, *, tenant: str = "acme") -> EventIn:
    return EventIn(
        tenant_id=tenant,
        source="batch.api",
        event_type="order.completed",
        idempotency_key=key,
        occurred_at=NOW,
        payload={"key": key},
    )


def payload(key: str, *, tenant: str = "acme") -> dict[str, object]:
    return event(key, tenant=tenant).model_dump(mode="json")


def test_storage_batch_preserves_order_and_mixes_duplicate_with_new_event(tmp_path: Path) -> None:
    database = Database(tmp_path / "events.db", clock=lambda: NOW)
    database.initialize()
    existing = database.ingest(event("batch-existing-01"))

    results = database.ingest_batch([event("batch-existing-01"), event("batch-new-0001")])

    assert results[0].event_id == existing.event_id
    assert results[0].duplicate is True
    assert results[0].queued is False
    assert results[1].duplicate is False
    assert results[1].queued is True
    assert database.counts() == {"events": 2, "jobs": 2, "projections": 0}


def test_storage_batch_rejects_empty_internal_call(tmp_path: Path) -> None:
    database = Database(tmp_path / "events.db", clock=lambda: NOW)
    database.initialize()
    with pytest.raises(ValueError, match="at least one event"):
        database.ingest_batch([])


def test_batch_api_returns_aggregate_outcomes_and_metrics(tmp_path: Path) -> None:
    path = tmp_path / "events.db"
    database = Database(path)
    database.initialize()
    existing = database.ingest(event("batch-api-existing"))
    client = TestClient(create_app(database_path=path))

    response = client.post(
        "/v1/events/batch",
        json={"events": [payload("batch-api-existing"), payload("batch-api-new001")]},
    )

    assert response.status_code == 202
    body = response.json()
    assert body["accepted"] == 1
    assert body["duplicates"] == 1
    assert body["queued"] == 1
    assert body["results"][0] == {"event_id": existing.event_id, "duplicate": True, "queued": False}
    assert body["results"][1]["duplicate"] is False
    metrics = client.get("/metrics").text
    assert "eventforge_events_accepted_total 1" in metrics
    assert "eventforge_events_duplicate_total 1" in metrics


def test_batch_api_rejects_cross_tenant_payload_without_writes(tmp_path: Path) -> None:
    path = tmp_path / "events.db"
    client = TestClient(create_app(database_path=path))

    response = client.post(
        "/v1/events/batch",
        json={"events": [payload("batch-tenant-001", tenant="acme"), payload("batch-tenant-002", tenant="globex")]},
    )

    assert response.status_code == 422
    assert Database(path).counts() == {"events": 0, "jobs": 0, "projections": 0}


def test_batch_api_enforces_maximum_batch_size(tmp_path: Path) -> None:
    client = TestClient(create_app(database_path=tmp_path / "events.db"))
    events = [payload(f"batch-limit-{index:04d}") for index in range(101)]
    response = client.post("/v1/events/batch", json={"events": events})
    assert response.status_code == 422
