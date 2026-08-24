from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from eventforge.api import create_app
from eventforge.security import TenantRateLimiter


def test_rate_limiter_is_tenant_scoped_and_expires_window() -> None:
    limiter = TenantRateLimiter(limit=2, window_seconds=10)
    limiter.check("acme", now=0)
    limiter.check("acme", now=1)
    limiter.check("globex", now=1)

    try:
        limiter.check("acme", now=2)
    except Exception as exc:
        assert getattr(exc, "code", None) == "rate_limit_exceeded"
    else:
        raise AssertionError("expected tenant limiter to reject the third request")

    limiter.check("acme", now=11)


def test_api_returns_429_with_stable_error_code_when_tenant_limit_is_exceeded(tmp_path: Path) -> None:
    client = TestClient(create_app(database_path=tmp_path / "events.db", rate_limit_per_minute=1))
    payload = {
        "tenant_id": "acme",
        "source": "sdk.python",
        "event_type": "user.active",
        "idempotency_key": "security-key-0001",
        "occurred_at": "2026-08-24T12:00:00Z",
        "payload": {},
    }
    assert client.post("/v1/events", json=payload).status_code == 202
    rejected = client.post("/v1/events", json=payload)
    assert rejected.status_code == 429
    assert rejected.json()["error"]["code"] == "rate_limit_exceeded"


def test_request_size_limit_rejects_body_before_validation(tmp_path: Path) -> None:
    client = TestClient(create_app(database_path=tmp_path / "events.db", max_request_bytes=1024))
    response = client.post(
        "/v1/events",
        content=b"x" * 2048,
        headers={"content-type": "application/json"},
    )
    assert response.status_code == 413
    assert response.json() == {"error": {"code": "request_too_large"}}


def test_api_adds_request_id_to_all_normal_responses(tmp_path: Path) -> None:
    client = TestClient(create_app(database_path=tmp_path / "events.db"))
    response = client.get("/healthz", headers={"x-request-id": "trace-123"})
    assert response.headers["x-request-id"] == "trace-123"
