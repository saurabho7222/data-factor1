"""FastAPI surface for ingestion, replay control, analytics, and observability."""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from dataclasses import asdict
from pathlib import Path
from typing import Annotated

import uvicorn
from fastapi import FastAPI, Query, Request, status
from fastapi.responses import JSONResponse, PlainTextResponse
from starlette.responses import Response

from . import __version__
from .analytics import analytics_summary, daily_metrics
from .api_models import (
    AnalyticsSummaryResponse,
    DailyMetricResponse,
    HealthResponse,
    IngestResponse,
    ReplayResponse,
)
from .errors import EventForgeError
from .metrics import Metrics
from .replay import enqueue_replay
from .schemas import AnalyticsFilter, EventIn, ReplayRequest
from .storage import SCHEMA_VERSION, Database

SERVICE_NAME = "eventforge"
DEFAULT_DB = Path(".local/eventforge.db")


def create_app(*, database_path: Path | None = None) -> FastAPI:
    database = Database(database_path or Path(os.environ.get("EVENTFORGE_DB", DEFAULT_DB)))
    database.initialize()
    metrics = Metrics()
    app = FastAPI(
        title="EventForge API",
        version=__version__,
        description="Durable multi-tenant event ingestion and replayable analytics.",
    )
    app.state.database = database
    app.state.metrics = metrics

    @app.middleware("http")
    async def count_requests(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        metrics.increment("http_requests_total")
        return await call_next(request)

    @app.exception_handler(EventForgeError)
    async def handle_domain_error(_request: Request, exc: EventForgeError) -> JSONResponse:
        metrics.increment("application_errors_total")
        return JSONResponse(
            status_code=exc.http_status,
            content={"error": {"code": exc.code, "message": str(exc)}},
        )

    @app.post("/v1/events", response_model=IngestResponse, status_code=status.HTTP_202_ACCEPTED)
    def ingest_event(event: EventIn) -> IngestResponse:
        result = database.ingest(event)
        metrics.increment("events_duplicate_total" if result.duplicate else "events_accepted_total")
        return IngestResponse(event_id=result.event_id, duplicate=result.duplicate, queued=result.queued)

    @app.post("/v1/replays", response_model=ReplayResponse, status_code=status.HTTP_202_ACCEPTED)
    def replay_events(request: ReplayRequest) -> ReplayResponse:
        queued = enqueue_replay(database, request)
        metrics.increment("replay_jobs_queued_total", queued)
        return ReplayResponse(queued=queued)

    @app.get("/v1/analytics/daily", response_model=list[DailyMetricResponse])
    def get_daily(filters: Annotated[AnalyticsFilter, Query()]) -> list[DailyMetricResponse]:
        return [DailyMetricResponse.model_validate(asdict(row)) for row in daily_metrics(database, filters)]

    @app.get("/v1/analytics/summary", response_model=AnalyticsSummaryResponse)
    def get_summary(filters: Annotated[AnalyticsFilter, Query()]) -> AnalyticsSummaryResponse:
        return AnalyticsSummaryResponse.model_validate(asdict(analytics_summary(database, filters)))

    @app.get("/healthz", response_model=HealthResponse)
    def health() -> HealthResponse:
        return HealthResponse(
            status="ok" if database.healthcheck() else "degraded",
            service=SERVICE_NAME,
            version=__version__,
            schema_version=SCHEMA_VERSION,
        )

    @app.get("/readyz", response_model=HealthResponse)
    def readiness() -> HealthResponse | JSONResponse:
        healthy = database.healthcheck()
        payload = HealthResponse(
            status="ready" if healthy else "not_ready",
            service=SERVICE_NAME,
            version=__version__,
            schema_version=SCHEMA_VERSION,
        )
        if healthy:
            return payload
        return JSONResponse(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content=payload.model_dump())

    @app.get("/metrics", response_class=PlainTextResponse)
    def prometheus_metrics() -> str:
        return metrics.render()

    return app


app = create_app()


def main() -> None:
    uvicorn.run(
        "eventforge.api:app",
        host=os.environ.get("EVENTFORGE_HOST", "0.0.0.0"),
        port=int(os.environ.get("EVENTFORGE_PORT", "8000")),
        log_level=os.environ.get("EVENTFORGE_LOG_LEVEL", "info").lower(),
    )
