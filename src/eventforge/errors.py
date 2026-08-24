"""Application error hierarchy with stable machine-readable codes."""

from __future__ import annotations


class EventForgeError(Exception):
    """Base class for expected domain/runtime failures."""

    code = "eventforge_error"
    http_status = 400


class AuthenticationFailed(EventForgeError):
    code = "authentication_failed"
    http_status = 401


class EventNotFound(EventForgeError):
    code = "event_not_found"
    http_status = 404


class RateLimitExceeded(EventForgeError):
    code = "rate_limit_exceeded"
    http_status = 429


class StorageUnavailable(EventForgeError):
    code = "storage_unavailable"
    http_status = 503


class SchemaVersionError(EventForgeError):
    code = "schema_version_error"
    http_status = 503
