"""Airflow operator for dynamic X-Auth-Token acquisition."""

from __future__ import annotations

from typing import Any, Sequence

from airflow.sdk import BaseOperator

from custom_operator.token.hooks.token import AuthTokenHook


class TokenOperator(BaseOperator):
    """Fetch an auth token and return it through XCom."""

    template_fields: Sequence[str] = (
        "token_base_url",
        "auth_url",
        "auth_body",
        "auth_headers",
    )

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
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.token_conn_id = token_conn_id
        self.token_base_url = token_base_url
        self.auth_url = auth_url
        self.auth_body = auth_body
        self.auth_headers = auth_headers
        self.request_timeout = request_timeout
        self.verify = verify

    def execute(self, context: dict[str, Any]) -> str:
        self.log.info("Fetching auth token task_id=%s", self.task_id)
        return AuthTokenHook(
            token_conn_id=self.token_conn_id,
            token_base_url=self.token_base_url,
            auth_url=self.auth_url,
            auth_body=self.auth_body,
            auth_headers=self.auth_headers,
            request_timeout=self.request_timeout,
            verify=self.verify,
        ).get_token()
