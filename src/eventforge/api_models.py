"""Typed HTTP response contracts for EventForge."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class IngestResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_id: str
    duplicate: bool
    queued: bool


class BatchIngestResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    accepted: int
    duplicates: int
    queued: int
    results: list[IngestResponse]


class ReplayResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    queued: int


class DailyMetricResponse(BaseModel):
    event_date: str
    event_type: str
    event_count: int
    payload_bytes: int


class AnalyticsSummaryResponse(BaseModel):
    projected_events: int
    distinct_event_types: int
    payload_bytes: int
    first_event_date: str | None
    last_event_date: str | None


class HealthResponse(BaseModel):
    status: str
    service: str
    version: str
    schema_version: int
