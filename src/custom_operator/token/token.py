"""X-Auth-Token provider helpers."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from custom_operator.token.exceptions import CustomOperatorAuthError
from custom_operator.token.http_client import HttpClient

log = logging.getLogger(__name__)


def mask_token(token: str | None) -> str:
    """Return a log-safe token preview."""

    if not token:
        return "<empty>"
    if len(token) < 5:
        return "***"
    return f"{token[:5]}***"


class TokenProvider:
    """Fetches X-Auth-Token values from an auth API response header."""

    def __init__(
        self,
        *,
        http_client: HttpClient,
        auth_url: str,
        auth_body: Mapping[str, Any],
    ) -> None:
        if not auth_url:
            raise CustomOperatorAuthError("auth_url must be configured")
        self.http_client = http_client
        self.auth_url = auth_url
        self.auth_body = dict(auth_body)
        self._token: str | None = None

    def get_token(self) -> str:
        if self._token:
            return self._token
        return self.refresh_token()

    def refresh_token(self) -> str:
        headers = {"Content-Type": "application/json"}
        log.info("Requesting auth token auth_url=%s", self.auth_url)
        response = self.http_client.request(
            "POST",
            self.auth_url,
            headers=headers,
            json=self.auth_body,
            expected_statuses={200, 201, 204},
            retry=0,
        )
        token = response.headers.get("x-subject-token")
        if not token:
            for key, value in response.headers.items():
                if key.lower() == "x-subject-token":
                    token = value
                    break
        if not token:
            raise CustomOperatorAuthError("token response header x-subject-token is missing")
        self._token = token
        log.info("Auth token acquired token=%s", mask_token(token))
        return token
