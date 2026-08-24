from __future__ import annotations

import json
import logging

from eventforge.logging_utils import JSON_LOG_SCHEMA, JsonLogFormatter, configure_json_logging


def test_json_log_formatter_matches_documented_required_schema() -> None:
    record = logging.LogRecord(
        name="eventforge.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="request completed",
        args=(),
        exc_info=None,
    )
    record.event = "http_request"
    record.request_id = "trace-123"
    record.status_code = 202
    payload = json.loads(JsonLogFormatter().format(record))
    required = set(JSON_LOG_SCHEMA["required"])
    assert required <= payload.keys()
    assert payload["event"] == "http_request"
    assert payload["request_id"] == "trace-123"
    assert payload["status_code"] == 202
    assert payload["level"] == "info"


def test_configure_json_logging_replaces_handlers_without_global_propagation() -> None:
    logger = logging.getLogger("eventforge.test.configure")
    logger.handlers[:] = [logging.NullHandler(), logging.NullHandler()]
    logger.propagate = True
    configure_json_logging(logger, level="WARNING")
    assert logger.level == logging.WARNING
    assert logger.propagate is False
    assert len(logger.handlers) == 1
    assert isinstance(logger.handlers[0].formatter, JsonLogFormatter)
