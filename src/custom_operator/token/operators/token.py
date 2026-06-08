"""Airflow operator for dynamic X-Auth-Token acquisition."""

from __future__ import annotations

from typing import Any, Sequence

from airflow.sdk import BaseOperator

from custom_operator.token.hooks.token import AuthTokenHook


class TokenOperator(BaseOperator):
    """Fetch an auth token and return it through XCom."""

    template_fields: Sequence[str] = (
        "auth_url",
        "auth_body",
    )

    def __init__(
        self,
        *,
        auth_url: str,
        auth_body: dict[str, Any],
        request_timeout: int = 30,
        verify: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.auth_url = auth_url
        self.auth_body = dict(auth_body)
        self.request_timeout = request_timeout
        self.verify = verify

    def execute(self, context: dict[str, Any]) -> str:
        self.log.info("Fetching auth token task_id=%s", self.task_id)
        return AuthTokenHook(
            auth_url=self.auth_url,
            auth_body=self.auth_body,
            request_timeout=self.request_timeout,
            verify=self.verify,
        ).get_token()
