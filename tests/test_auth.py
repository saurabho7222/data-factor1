from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from eventforge.api import create_app
from eventforge.auth import TenantAuthenticator
from eventforge.errors import AuthenticationFailed

AUTH_SECRET = "test-authentication-secret-with-at-least-32-bytes"


def event_payload(*, tenant: str = "acme") -> dict[str, object]:
    return {
        "tenant_id": tenant,
        "source": "auth.test",
        "event_type": "security.checked",
        "idempotency_key": f"auth-event-{tenant}-0001",
        "occurred_at": "2026-08-24T12:00:00Z",
        "payload": {},
    }


def token_for(tenant: str) -> str:
    return TenantAuthenticator(AUTH_SECRET).token_for(tenant)


def test_authenticator_requires_strong_secret_and_derives_tenant_specific_tokens() -> None:
    with pytest.raises(ValueError, match="at least 32 bytes"):
        TenantAuthenticator("too-short")

    authenticator = TenantAuthenticator(AUTH_SECRET)
    acme = authenticator.token_for("acme")
    globex = authenticator.token_for("globex")
    assert len(acme) == 64
    assert acme != globex
    authenticator.verify("acme", acme)

    with pytest.raises(AuthenticationFailed, match="invalid tenant authentication token"):
        authenticator.verify("acme", globex)
    with pytest.raises(AuthenticationFailed):
        authenticator.verify("acme", None)


def test_authenticated_api_rejects_missing_wrong_and_cross_tenant_tokens(tmp_path: Path) -> None:
    client = TestClient(create_app(database_path=tmp_path / "events.db", auth_secret=AUTH_SECRET))
    payload = event_payload()

    missing = client.post("/v1/events", json=payload)
    wrong = client.post("/v1/events", json=payload, headers={"X-EventForge-Token": token_for("globex")})
    accepted = client.post("/v1/events", json=payload, headers={"X-EventForge-Token": token_for("acme")})
    cross_tenant = client.get(
        "/v1/analytics/summary",
        params={"tenant_id": "acme"},
        headers={"X-EventForge-Token": token_for("globex")},
    )

    assert missing.status_code == 401
    assert missing.json()["error"]["code"] == "authentication_failed"
    assert wrong.status_code == 401
    assert accepted.status_code == 202
    assert cross_tenant.status_code == 401


def test_operational_health_and_metrics_remain_available_without_tenant_token(tmp_path: Path) -> None:
    client = TestClient(create_app(database_path=tmp_path / "events.db", auth_secret=AUTH_SECRET))
    assert client.get("/healthz").status_code == 200
    assert client.get("/readyz").status_code == 200
    assert client.get("/metrics").status_code == 200
