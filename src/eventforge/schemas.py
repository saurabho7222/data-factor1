"""Canonical request/configuration schemas for EventForge trust boundaries."""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator

TenantId = Annotated[str, Field(min_length=2, max_length=64, pattern=r"^[a-z0-9][a-z0-9_-]*$")]
SourceName = Annotated[str, Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")]
EventType = Annotated[str, Field(min_length=1, max_length=96, pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")]
IdempotencyKey = Annotated[str, Field(min_length=8, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")]
MAX_PAYLOAD_BYTES = 65_536


class EventIn(BaseModel):
    """Validated immutable event envelope accepted by ingestion."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    tenant_id: TenantId
    source: SourceName
    event_type: EventType
    idempotency_key: IdempotencyKey
    occurred_at: datetime
    payload: dict[str, JsonValue] = Field(default_factory=dict)

    @field_validator("occurred_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("occurred_at must include a timezone offset")
        return value

    @field_validator("payload")
    @classmethod
    def bound_payload_size(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        encoded = json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        if len(encoded) > MAX_PAYLOAD_BYTES:
            raise ValueError(f"payload must be <= {MAX_PAYLOAD_BYTES} encoded bytes")
        return value


class ReplayRequest(BaseModel):
    """Bounded selector for replaying immutable raw events."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    tenant_id: TenantId
    event_type: EventType | None = None
    start_date: date | None = None
    end_date: date | None = None
    limit: int = Field(default=1000, ge=1, le=5000)

    @model_validator(mode="after")
    def validate_range(self) -> ReplayRequest:
        if self.start_date is not None and self.end_date is not None and self.start_date > self.end_date:
            raise ValueError("start_date must be on or before end_date")
        return self


class AnalyticsFilter(BaseModel):
    """Tenant-scoped filter for projected analytics."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    tenant_id: TenantId
    event_type: EventType | None = None
    start_date: date | None = None
    end_date: date | None = None

    @model_validator(mode="after")
    def validate_range(self) -> AnalyticsFilter:
        if self.start_date is not None and self.end_date is not None and self.start_date > self.end_date:
            raise ValueError("start_date must be on or before end_date")
        return self
