from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from eventforge.analytics import analytics_summary, daily_metrics
from eventforge.processing import Worker
from eventforge.schemas import AnalyticsFilter, EventIn
from eventforge.storage import Database

BASE = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)


def ingest(database: Database, tenant: str, kind: str, day: int, key: str) -> None:
    database.ingest(
        EventIn(
            tenant_id=tenant,
            source="sdk.python",
            event_type=kind,
            idempotency_key=key,
            occurred_at=BASE + timedelta(days=day),
            payload={"key": key, "nested": {"ok": True}},
        )
    )


def drain(database: Database) -> None:
    worker = Worker(database)
    now = BASE + timedelta(days=20)
    while worker.process_one(now=now):
        pass


def test_analytics_only_reads_projected_events_and_isolates_tenants(tmp_path: Path) -> None:
    database = Database(tmp_path / "events.db")
    database.initialize()
    ingest(database, "acme", "order.completed", 0, "acme-event-0001")
    ingest(database, "globex", "order.completed", 0, "globex-event-001")

    before = analytics_summary(database, AnalyticsFilter(tenant_id="acme"))
    assert before.projected_events == 0

    drain(database)
    acme = analytics_summary(database, AnalyticsFilter(tenant_id="acme"))
    globex = analytics_summary(database, AnalyticsFilter(tenant_id="globex"))
    assert acme.projected_events == 1
    assert globex.projected_events == 1


def test_daily_metrics_group_by_date_and_type_deterministically(tmp_path: Path) -> None:
    database = Database(tmp_path / "events.db")
    database.initialize()
    ingest(database, "acme", "order.completed", 0, "analytics-key-01")
    ingest(database, "acme", "order.completed", 0, "analytics-key-02")
    ingest(database, "acme", "order.refunded", 1, "analytics-key-03")
    drain(database)

    rows = daily_metrics(database, AnalyticsFilter(tenant_id="acme"))
    assert [(row.event_date, row.event_type, row.event_count) for row in rows] == [
        ("2026-08-20", "order.completed", 2),
        ("2026-08-21", "order.refunded", 1),
    ]
    assert all(row.payload_bytes > 0 for row in rows)


def test_analytics_filters_type_and_inclusive_date_range(tmp_path: Path) -> None:
    database = Database(tmp_path / "events.db")
    database.initialize()
    ingest(database, "acme", "order.completed", 0, "filter-event-001")
    ingest(database, "acme", "order.refunded", 1, "filter-event-002")
    ingest(database, "acme", "order.completed", 2, "filter-event-003")
    drain(database)

    filters = AnalyticsFilter(
        tenant_id="acme",
        event_type="order.completed",
        start_date="2026-08-21",
        end_date="2026-08-22",
    )
    rows = daily_metrics(database, filters)
    summary = analytics_summary(database, filters)
    assert [(row.event_date, row.event_count) for row in rows] == [("2026-08-22", 1)]
    assert summary.projected_events == 1
    assert summary.distinct_event_types == 1
    assert summary.first_event_date == "2026-08-22"
    assert summary.last_event_date == "2026-08-22"


def test_empty_summary_has_stable_zero_and_null_values(tmp_path: Path) -> None:
    database = Database(tmp_path / "events.db")
    database.initialize()
    summary = analytics_summary(database, AnalyticsFilter(tenant_id="acme"))
    assert summary.projected_events == 0
    assert summary.distinct_event_types == 0
    assert summary.payload_bytes == 0
    assert summary.first_event_date is None
    assert summary.last_event_date is None
