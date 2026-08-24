"""Tenant-bound HMAC authentication for the HTTP trust boundary."""

from __future__ import annotations

import hashlib
import hmac

from .errors import AuthenticationFailed

MIN_AUTH_SECRET_BYTES = 32


class TenantAuthenticator:
    """Derive and verify tenant-specific tokens from one deployment secret."""

    def __init__(self, secret: str) -> None:
        key = secret.encode("utf-8")
        if len(key) < MIN_AUTH_SECRET_BYTES:
            raise ValueError(f"authentication secret must be at least {MIN_AUTH_SECRET_BYTES} bytes")
        self._key = key

    def token_for(self, tenant_id: str) -> str:
        return hmac.new(self._key, tenant_id.encode("utf-8"), hashlib.sha256).hexdigest()

    def verify(self, tenant_id: str, token: str | None) -> None:
        expected = self.token_for(tenant_id)
        if token is None or not hmac.compare_digest(expected, token):
            raise AuthenticationFailed("invalid tenant authentication token")
