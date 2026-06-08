"""Airflow hook for dynamic X-Auth-Token acquisition."""

from __future__ import annotations

from typing import Any

from airflow.sdk import BaseHook

from custom_operator.token.http_client import HttpClient
from custom_operator.token.token import TokenProvider


class AuthTokenHook(BaseHook):
    """Fetch tokens from an auth API without depending on Spark or Ray modules."""

    hook_name = "AiDatalake Token"

    def __init__(
        self,
        *,
        auth_url: str,
        auth_body: dict[str, Any],
        request_timeout: int = 30,
        verify: bool = True,
    ) -> None:
        super().__init__()
        self.auth_url = auth_url
        self.auth_body = dict(auth_body)
        self.request_timeout = request_timeout
        self.verify = verify

    def get_token(self) -> str:
        provider = TokenProvider(
            http_client=HttpClient(
                timeout=self.request_timeout,
                verify=self.verify,
            ),
            auth_url=self.auth_url,
            auth_body=self.auth_body,
        )
        return provider.get_token()
