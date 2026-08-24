"""FastAPI surface for ingestion, recovery, analytics, and observability."""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Awaitable, Callable
from dataclasses import asdict
from pathlib import Path
from typing import Annotated
from uuid import uuid4

import uvicorn
from fastapi import FastAPI, Header, Query, Request, status
from fastapi.responses import JSONResponse, PlainTextResponse
from starlette.responses import Response

from . import __version__
from .analytics import analytics_summary, daily_metrics
from .api_models import (
    AnalyticsSummaryResponse,
    BatchIngestResponse,
    DailyMetricResponse,
    DeadLetterResponse,
    DeadLetterRetryResponse,
    HealthResponse,
    IngestResponse,
    ReplayResponse,
)
from .auth import TenantAuthenticator
from .dead_letters import list_dead_letters, retry_dead_letters
from .errors import EventForgeError
from .logging_utils import configure_json_logging
from .metrics import Metrics
from .replay import enqueue_replay
from .schemas import AnalyticsFilter, DeadLetterFilter, EventBatchIn, EventIn, ReplayRequest
from .security import RequestSizeLimitMiddleware, TenantRateLimiter
from .storage import SCHEMA_VERSION, Database

SERVICE_NAME = "eventforge"
DEFAULT_DB = Path(".local/eventforge.db")
LOGGER = logging.getLogger("eventforge.api")
TenantToken = Annotated[str | None, Header(alias="X-EventForge-Token")]


def create_app(
    *,
    database_path: Path | None = None,
    max_request_bytes: int = 262_144,
    rate_limit_per_minute: int = 120,
    auth_secret: str | None = None,
) -> FastAPI:
    database = Database(database_path or Path(os.environ.get("EVENTFORGE_DB", DEFAULT_DB)))
    database.initialize()
    metrics = Metrics()
    limiter = TenantRateLimiter(limit=rate_limit_per_minute)
    authenticator = TenantAuthenticator(auth_secret) if auth_secret is not None else None
    app = FastAPI(
        title="EventForge API",
        version=__version__,
        description="Durable authenticated multi-tenant event ingestion and replayable analytics.",
    )
    app.state.database = database
    app.state.metrics = metrics
    app.state.authenticator = authenticator
    app.add_middleware(RequestSizeLimitMiddleware, max_bytes=max_request_bytes)

    def authorize(tenant_id: str, token: str | None) -> None:
        if authenticator is not None:
            authenticator.verify(tenant_id, token)

    @app.middleware("http")
    async def observe_request(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        metrics.increment("http_requests_total")
        started = time.perf_counter()
        request_id = request.headers.get("x-request-id") or str(uuid4())
        response = await call_next(request)
        response.headers["x-request-id"] = request_id
        LOGGER.info(
            "request completed",
            extra={
                "event": "http_request",
                "request_id": request_id,
                "status_code": response.status_code,
                "duration_ms": round((time.perf_counter() - started) * 1000, 3),
            },
        )
        return response

    @app.exception_handler(EventForgeError)
    async def handle_domain_error(_request: Request, exc: EventForgeError) -> JSONResponse:
        metrics.increment("application_errors_total")
        return JSONResponse(
            status_code=exc.http_status,
            content={"error": {"code": exc.code, "message": str(exc)}},
        )

    @app.post("/v1/events", response_model=IngestResponse, status_code=status.HTTP_202_ACCEPTED)
    def ingest_event(event: EventIn, token: TenantToken = None) -> IngestResponse:
        authorize(event.tenant_id, token)
        limiter.check(event.tenant_id)
        result = database.ingest(event)
        metrics.increment("events_duplicate_total" if result.duplicate else "events_accepted_total")
        return IngestResponse(event_id=result.event_id, duplicate=result.duplicate, queued=result.queued)

    @app.post("/v1/events/batch", response_model=BatchIngestResponse, status_code=status.HTTP_202_ACCEPTED)
    def ingest_event_batch(batch: EventBatchIn, token: TenantToken = None) -> BatchIngestResponse:
        authorize(batch.tenant_id, token)
        limiter.check(batch.tenant_id)
        results = database.ingest_batch(batch.events)
        accepted = sum(1 for result in results if not result.duplicate)
        duplicates = sum(1 for result in results if result.duplicate)
        queued = sum(1 for result in results if result.queued)
        metrics.increment("events_accepted_total", accepted)
        metrics.increment("events_duplicate_total", duplicates)
        return BatchIngestResponse(
            accepted=accepted,
            duplicates=duplicates,
            queued=queued,
            results=[
                IngestResponse(event_id=result.event_id, duplicate=result.duplicate, queued=result.queued)
                for result in results
            ],
        )

    @app.post("/v1/replays", response_model=ReplayResponse, status_code=status.HTTP_202_ACCEPTED)
    def replay_events(request: ReplayRequest, token: TenantToken = None) -> ReplayResponse:
        authorize(request.tenant_id, token)
        limiter.check(request.tenant_id)
        queued = enqueue_replay(database, request)
        metrics.increment("replay_jobs_queued_total", queued)
        return ReplayResponse(queued=queued)

    @app.get("/v1/dead-letters", response_model=list[DeadLetterResponse])
    def get_dead_letters(
        filters: Annotated[DeadLetterFilter, Query()],
        token: TenantToken = None,
    ) -> list[DeadLetterResponse]:
        authorize(filters.tenant_id, token)
        limiter.check(filters.tenant_id)
        return [
            DeadLetterResponse.model_validate(asdict(row))
            for row in list_dead_letters(database, tenant_id=filters.tenant_id, limit=filters.limit)
        ]

    @app.post("/v1/dead-letters/retry", response_model=DeadLetterRetryResponse, status_code=status.HTTP_202_ACCEPTED)
    def retry_failed_jobs(request: DeadLetterFilter, token: TenantToken = None) -> DeadLetterRetryResponse:
        authorize(request.tenant_id, token)
        limiter.check(request.tenant_id)
        requeued = retry_dead_letters(database, tenant_id=request.tenant_id, limit=request.limit)
        metrics.increment("dead_letter_jobs_requeued_total", requeued)
        return DeadLetterRetryResponse(requeued=requeued)

    @app.get("/v1/analytics/daily", response_model=list[DailyMetricResponse])
    def get_daily(
        filters: Annotated[AnalyticsFilter, Query()],
        token: TenantToken = None,
    ) -> list[DailyMetricResponse]:
        authorize(filters.tenant_id, token)
        limiter.check(filters.tenant_id)
        return [DailyMetricResponse.model_validate(asdict(row)) for row in daily_metrics(database, filters)]

    @app.get("/v1/analytics/summary", response_model=AnalyticsSummaryResponse)
    def get_summary(
        filters: Annotated[AnalyticsFilter, Query()],
        token: TenantToken = None,
    ) -> AnalyticsSummaryResponse:
        authorize(filters.tenant_id, token)
        limiter.check(filters.tenant_id)
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
        return payload if healthy else JSONResponse(status_code=503, content=payload.model_dump())

    @app.get("/metrics", response_class=PlainTextResponse)
    def prometheus_metrics() -> str:
        return metrics.render()

    return app


def app_factory() -> FastAPI:
    """Create the production ASGI app and require a deployment authentication secret."""
    secret = os.environ.get("EVENTFORGE_AUTH_SECRET")
    if secret is None:
        raise RuntimeError("EVENTFORGE_AUTH_SECRET is required to start the API service")
    return create_app(auth_secret=secret)


def main() -> None:
    configure_json_logging(LOGGER, level=os.environ.get("EVENTFORGE_LOG_LEVEL", "INFO"))
    uvicorn.run(
        "eventforge.api:app_factory",
        factory=True,
        host=os.environ.get("EVENTFORGE_HOST", "0.0.0.0"),
        port=int(os.environ.get("EVENTFORGE_PORT", "8000")),
        log_level=os.environ.get("EVENTFORGE_LOG_LEVEL", "info").lower(),
    )
