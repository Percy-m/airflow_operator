"""Airflow hook for dynamic X-Auth-Token acquisition."""

from __future__ import annotations

from typing import Any

from airflow.sdk import BaseHook

from custom_operator.token.connection import resolve_auth_connection_config
from custom_operator.token.http_client import HttpClient
from custom_operator.token.token import TokenProvider


class AuthTokenHook(BaseHook):
    """Fetch tokens from an auth API without depending on Spark or Ray modules."""

    conn_name_attr = "token_conn_id"
    default_conn_name = "aidatalake_token"
    conn_type = "http"
    hook_name = "AiDatalake Token"

    def __init__(
        self,
        *,
        token_conn_id: str | None = None,
        token_base_url: str | None = None,
        auth_url: str | None = None,
        auth_body: dict[str, Any] | None = None,
        auth_headers: dict[str, str] | None = None,
        request_timeout: int = 30,
        verify: bool = True,
    ) -> None:
        super().__init__()
        self.token_conn_id = token_conn_id or self.default_conn_name
        self.token_base_url = token_base_url
        self.auth_url = auth_url
        self.auth_body = dict(auth_body or {})
        self.auth_headers = dict(auth_headers or {})
        self.request_timeout = request_timeout
        self.verify = verify

    def get_token(self) -> str:
        config = resolve_auth_connection_config(
            self,
            conn_id=self.token_conn_id,
            base_url=self.token_base_url,
            auth_url=self.auth_url,
            auth_body=self.auth_body,
            auth_headers=self.auth_headers,
            request_timeout=self.request_timeout,
            verify=self.verify,
        )
        provider = TokenProvider(
            http_client=HttpClient(
                base_url=config.base_url,
                timeout=config.timeout,
                verify=config.verify,
            ),
            auth_url=config.auth_url,
            auth_body=config.auth_body,
            auth_headers=config.auth_headers,
        )
        return provider.get_token()
