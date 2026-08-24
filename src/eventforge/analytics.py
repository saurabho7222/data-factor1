"""Read-only tenant-scoped analytics over durable event projections."""

from __future__ import annotations

from dataclasses import dataclass

from .schemas import AnalyticsFilter
from .storage import Database


@dataclass(frozen=True, slots=True)
class DailyMetric:
    event_date: str
    event_type: str
    event_count: int
    payload_bytes: int


@dataclass(frozen=True, slots=True)
class AnalyticsSummary:
    projected_events: int
    distinct_event_types: int
    payload_bytes: int
    first_event_date: str | None
    last_event_date: str | None


def _params(filters: AnalyticsFilter) -> tuple[object, ...]:
    start = filters.start_date.isoformat() if filters.start_date else None
    end = filters.end_date.isoformat() if filters.end_date else None
    return (
        filters.tenant_id,
        filters.event_type,
        filters.event_type,
        start,
        start,
        end,
        end,
    )


def daily_metrics(database: Database, filters: AnalyticsFilter) -> list[DailyMetric]:
    with database.connect() as connection:
        rows = connection.execute(
            """
            SELECT event_date, event_type, COUNT(*) AS event_count, SUM(payload_bytes) AS payload_bytes
            FROM projections
            WHERE tenant_id = ?
              AND (? IS NULL OR event_type = ?)
              AND (? IS NULL OR event_date >= ?)
              AND (? IS NULL OR event_date <= ?)
            GROUP BY event_date, event_type
            ORDER BY event_date, event_type
            """,
            _params(filters),
        ).fetchall()
    return [
        DailyMetric(
            event_date=str(row["event_date"]),
            event_type=str(row["event_type"]),
            event_count=int(row["event_count"]),
            payload_bytes=int(row["payload_bytes"]),
        )
        for row in rows
    ]


def analytics_summary(database: Database, filters: AnalyticsFilter) -> AnalyticsSummary:
    with database.connect() as connection:
        row = connection.execute(
            """
            SELECT
                COUNT(*) AS projected_events,
                COUNT(DISTINCT event_type) AS distinct_event_types,
                COALESCE(SUM(payload_bytes), 0) AS payload_bytes,
                MIN(event_date) AS first_event_date,
                MAX(event_date) AS last_event_date
            FROM projections
            WHERE tenant_id = ?
              AND (? IS NULL OR event_type = ?)
              AND (? IS NULL OR event_date >= ?)
              AND (? IS NULL OR event_date <= ?)
            """,
            _params(filters),
        ).fetchone()
    assert row is not None
    return AnalyticsSummary(
        projected_events=int(row["projected_events"]),
        distinct_event_types=int(row["distinct_event_types"]),
        payload_bytes=int(row["payload_bytes"]),
        first_event_date=str(row["first_event_date"]) if row["first_event_date"] is not None else None,
        last_event_date=str(row["last_event_date"]) if row["last_event_date"] is not None else None,
    )
