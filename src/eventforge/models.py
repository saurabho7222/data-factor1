"""Internal immutable records shared by storage and processing layers."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class IngestResult:
    event_id: str
    duplicate: bool
    queued: bool


@dataclass(frozen=True, slots=True)
class JobRecord:
    job_id: int
    event_id: str
    tenant_id: str
    kind: str
    attempts: int
