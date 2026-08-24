"""Stable structured logging contract for API and worker diagnostics."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Final

STRUCTURED_FIELDS: Final[tuple[str, ...]] = (
    "request_id",
    "tenant_id",
    "event_id",
    "job_id",
    "status_code",
    "duration_ms",
    "error_type",
)
JSON_LOG_SCHEMA: Final[dict[str, object]] = {
    "schema_version": 1,
    "required": ("timestamp", "level", "logger", "event", "message"),
    "optional": STRUCTURED_FIELDS,
}


class JsonLogFormatter(logging.Formatter):
    """Emit one deterministic JSON object per log record."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname.lower(),
            "logger": record.name,
            "event": getattr(record, "event", "log"),
            "message": record.getMessage(),
        }
        for field in STRUCTURED_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def configure_json_logging(logger: logging.Logger, *, level: str = "INFO") -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonLogFormatter())
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(getattr(logging, level.upper()))
    logger.propagate = False
