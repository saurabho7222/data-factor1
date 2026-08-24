from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from eventforge.schemas import AnalyticsFilter, EventIn, MAX_PAYLOAD_BYTES, ReplayRequest


def valid_event(**overrides: object) -> EventIn:
    payload: dict[str, object] = {
        "tenant_id": "acme",
        "source": "checkout.api",
        "event_type": "order.completed",
        "idempotency_key": "order-2026-0001",
        "occurred_at": datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc),
        "payload": {"order_id": "o-1", "amount": 42.5},
    }
    payload.update(overrides)
    return EventIn.model_validate(payload)


def test_event_schema_accepts_timezone_aware_json_payload() -> None:
    event = valid_event()
    assert event.tenant_id == "acme"
    assert event.payload["amount"] == 42.5


@pytest.mark.parametrize("tenant_id", ["A", "UpperCase", "-leading", "contains space"])
def test_event_schema_rejects_invalid_tenant_ids(tenant_id: str) -> None:
    with pytest.raises(ValidationError):
        valid_event(tenant_id=tenant_id)


def test_event_schema_requires_timezone_aware_timestamp() -> None:
    with pytest.raises(ValidationError, match="timezone offset"):
        valid_event(occurred_at=datetime(2026, 8, 24, 12, 0))


def test_event_schema_bounds_encoded_payload_size() -> None:
    with pytest.raises(ValidationError, match="payload must be"):
        valid_event(payload={"blob": "x" * (MAX_PAYLOAD_BYTES + 1)})


def test_event_schema_forbids_unknown_fields() -> None:
    with pytest.raises(ValidationError, match="extra_forbidden"):
        valid_event(unexpected=True)


def test_replay_request_rejects_reversed_dates_and_unbounded_limit() -> None:
    with pytest.raises(ValidationError, match="start_date must be"):
        ReplayRequest(tenant_id="acme", start_date="2026-08-25", end_date="2026-08-24")
    with pytest.raises(ValidationError):
        ReplayRequest(tenant_id="acme", limit=5001)


def test_analytics_filter_rejects_reversed_dates() -> None:
    with pytest.raises(ValidationError, match="start_date must be"):
        AnalyticsFilter(tenant_id="acme", start_date="2026-08-25", end_date="2026-08-24")
