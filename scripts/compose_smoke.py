"""End-to-end smoke test for the authenticated Docker Compose API + worker stack."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping
from typing import Any

BASE = "http://127.0.0.1:8000"
AUTH_SECRET = os.environ.get(
    "EVENTFORGE_AUTH_SECRET",
    "eventforge-local-development-secret-not-production",
)


def tenant_token(tenant_id: str) -> str:
    return hmac.new(AUTH_SECRET.encode("utf-8"), tenant_id.encode("utf-8"), hashlib.sha256).hexdigest()


def tenant_headers(tenant_id: str) -> dict[str, str]:
    return {"X-EventForge-Token": tenant_token(tenant_id)}


def get_json(path: str, *, tenant_id: str | None = None) -> Any:
    request = urllib.request.Request(BASE + path, headers=tenant_headers(tenant_id) if tenant_id else {})
    with urllib.request.urlopen(request, timeout=3) as response:
        return json.load(response)


def post_json(path: str, payload: Mapping[str, object], *, tenant_id: str) -> Any:
    headers = {"content-type": "application/json", **tenant_headers(tenant_id)}
    request = urllib.request.Request(
        BASE + path,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.load(response)


def wait_until_ready() -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            if get_json("/readyz")["status"] == "ready":
                return
        except (OSError, urllib.error.URLError, KeyError):
            pass
        time.sleep(0.5)
    raise RuntimeError("EventForge API did not become ready")


def wait_for_projection() -> None:
    query = urllib.parse.urlencode({"tenant_id": "demo"})
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        summary = get_json(f"/v1/analytics/summary?{query}", tenant_id="demo")
        if summary["projected_events"] == 1:
            return
        time.sleep(0.2)
    raise RuntimeError("worker did not project the demo event")


def main() -> None:
    wait_until_ready()
    event: dict[str, object] = {
        "tenant_id": "demo",
        "source": "compose.smoke",
        "event_type": "demo.completed",
        "idempotency_key": "compose-smoke-0001",
        "occurred_at": "2026-08-24T12:00:00Z",
        "payload": {"source": "compose", "ok": True},
    }
    first = post_json("/v1/events", event, tenant_id="demo")
    duplicate = post_json("/v1/events", event, tenant_id="demo")
    assert first["duplicate"] is False and first["queued"] is True
    assert duplicate["duplicate"] is True and duplicate["event_id"] == first["event_id"]
    wait_for_projection()

    query = urllib.parse.urlencode({"tenant_id": "demo"})
    daily = get_json(f"/v1/analytics/daily?{query}", tenant_id="demo")
    assert daily[0]["event_count"] == 1

    replay = post_json("/v1/replays", {"tenant_id": "demo", "limit": 10}, tenant_id="demo")
    assert replay["queued"] == 1
    metrics = urllib.request.urlopen(BASE + "/metrics", timeout=3).read().decode("utf-8")
    assert "eventforge_events_accepted_total 1" in metrics
    assert "eventforge_events_duplicate_total 1" in metrics
    assert "eventforge_replay_jobs_queued_total 1" in metrics
    print("compose smoke: ok")


if __name__ == "__main__":
    main()
