"""Network trust-boundary controls for request size and tenant request rate."""

from __future__ import annotations

import json
import time
from collections import defaultdict, deque
from threading import Lock

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from .errors import RateLimitExceeded


class TenantRateLimiter:
    """Thread-safe fixed-window limiter keyed by validated tenant identifiers."""

    def __init__(self, *, limit: int, window_seconds: float = 60.0) -> None:
        if limit < 1 or limit > 100_000:
            raise ValueError("limit must be between 1 and 100000")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self.limit = limit
        self.window_seconds = window_seconds
        self._hits: dict[str, deque[float]] = defaultdict(deque)
        self._lock = Lock()

    def check(self, tenant_id: str, *, now: float | None = None) -> None:
        current = time.monotonic() if now is None else now
        cutoff = current - self.window_seconds
        with self._lock:
            bucket = self._hits[tenant_id]
            while bucket and bucket[0] <= cutoff:
                bucket.popleft()
            if len(bucket) >= self.limit:
                raise RateLimitExceeded("tenant request rate exceeded")
            bucket.append(current)


class _RequestTooLarge(Exception):
    pass


class RequestSizeLimitMiddleware:
    """Reject oversized HTTP request bodies even when Content-Length is absent."""

    def __init__(self, app: ASGIApp, max_bytes: int = 262_144) -> None:
        if max_bytes < 1024:
            raise ValueError("max_bytes must be at least 1024")
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = {key.lower(): value for key, value in scope.get("headers", [])}
        content_length = headers.get(b"content-length")
        if content_length is not None:
            try:
                if int(content_length) > self.max_bytes:
                    await self._reject(send)
                    return
            except ValueError:
                await self._reject(send, status_code=400, code="invalid_content_length")
                return

        consumed = 0

        async def limited_receive() -> Message:
            nonlocal consumed
            message = await receive()
            if message["type"] == "http.request":
                consumed += len(message.get("body", b""))
                if consumed > self.max_bytes:
                    raise _RequestTooLarge
            return message

        try:
            await self.app(scope, limited_receive, send)
        except _RequestTooLarge:
            await self._reject(send)

    @staticmethod
    async def _reject(send: Send, *, status_code: int = 413, code: str = "request_too_large") -> None:
        body = json.dumps({"error": {"code": code}}, separators=(",", ":")).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": status_code,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode("ascii")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})
