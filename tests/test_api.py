from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from eventforge.api import create_app
from eventforge.processing import Worker
from eventforge.storage import Database


def event_payload(*, key: str = "api-event-0001", tenant: str = "acme") -> dict[str, object]:
    return {
        "tenant_id": tenant,
        "source": "checkout.api",
        "event_type": "order.completed",
        "idempotency_key": key,
        "occurred_at": "2026-08-24T12:00:00Z",
        "payload": {"order_id": "o-1", "amount": 42.5},
    }


def test_ingestion_api_is_idempotent_and_metrics_expose_outcome(tmp_path: Path) -> None:
    path = tmp_path / "events.db"
    client = TestClient(create_app(database_path=path))
    first = client.post("/v1/events", json=event_payload())
    duplicate = client.post("/v1/events", json=event_payload())
    assert first.status_code == 202
    assert duplicate.status_code == 202
    assert duplicate.json() == {
        "event_id": first.json()["event_id"],
        "duplicate": True,
        "queued": False,
    }
    metrics = client.get("/metrics").text
    assert "eventforge_events_accepted_total 1" in metrics
    assert "eventforge_events_duplicate_total 1" in metrics
    assert "eventforge_http_requests_total 3" in metrics


def test_worker_projection_becomes_visible_through_analytics_api(tmp_path: Path) -> None:
    path = tmp_path / "events.db"
    client = TestClient(create_app(database_path=path))
    client.post("/v1/events", json=event_payload())
    database = Database(path)
    assert Worker(database).process_one(now=datetime.now(UTC) + timedelta(seconds=1)) is True

    daily = client.get("/v1/analytics/daily", params={"tenant_id": "acme"})
    summary = client.get("/v1/analytics/summary", params={"tenant_id": "acme"})
    assert daily.status_code == 200
    assert daily.json()[0]["event_count"] == 1
    assert summary.json()["projected_events"] == 1
    assert summary.json()["distinct_event_types"] == 1


def test_replay_api_queues_matching_events(tmp_path: Path) -> None:
    path = tmp_path / "events.db"
    client = TestClient(create_app(database_path=path))
    client.post("/v1/events", json=event_payload())
    response = client.post("/v1/replays", json={"tenant_id": "acme", "event_type": "order.completed"})
    assert response.status_code == 202
    assert response.json() == {"queued": 1}
    assert "eventforge_replay_jobs_queued_total 1" in client.get("/metrics").text


def test_health_readiness_and_openapi_are_machine_readable(tmp_path: Path) -> None:
    client = TestClient(create_app(database_path=tmp_path / "events.db"))
    health = client.get("/healthz")
    ready = client.get("/readyz")
    schema = client.get("/openapi.json").json()
    assert health.status_code == 200
    assert health.json()["status"] == "ok"
    assert health.json()["schema_version"] == 1
    assert ready.json()["status"] == "ready"
    assert "/v1/events" in schema["paths"]
    assert "/metrics" in schema["paths"]


def test_invalid_event_and_query_boundaries_return_422(tmp_path: Path) -> None:
    client = TestClient(create_app(database_path=tmp_path / "events.db"))
    invalid = event_payload()
    invalid["tenant_id"] = "INVALID TENANT"
    assert client.post("/v1/events", json=invalid).status_code == 422
    assert client.get(
        "/v1/analytics/daily",
        params={"tenant_id": "acme", "start_date": "2026-08-25", "end_date": "2026-08-24"},
    ).status_code == 422
