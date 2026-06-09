"""Common hook base classes for AiDatalake compute jobs."""

from __future__ import annotations

from typing import Any, ClassVar

from airflow.sdk import BaseHook

from custom_operator.common.connection import resolve_connection_config
from custom_operator.common.exceptions import AiDatalakeApiError, AiDatalakeAuthError
from custom_operator.common.http_client import HttpClient
from custom_operator.common.token import StaticTokenProvider


class BaseComputeHook(BaseHook):
    """Build a compute API client from direct config or an Airflow Connection."""

    api_client_cls: ClassVar[type]
    api_error_cls: ClassVar[type[AiDatalakeApiError]] = AiDatalakeApiError
    auth_error_cls: ClassVar[type[Exception]] = AiDatalakeAuthError
    service_name: ClassVar[str] = "AiDatalake"

    def __init__(
        self,
        *,
        conn_id: str,
        workspace_id: str,
        base_url: str | None = None,
        token: str | None = None,
        request_timeout: int = 30,
        verify: bool = True,
    ) -> None:
        super().__init__()
        self.conn_id = conn_id
        self.workspace_id = workspace_id
        self.base_url = base_url
        self.token = token
        self.request_timeout = request_timeout
        self.verify = verify
        self._client: Any | None = None

    @property
    def client(self):
        if self._client is None:
            self._client = self._build_client()
        return self._client

    def _build_client(self):
        config = resolve_connection_config(
            self,
            conn_id=self.conn_id,
            base_url=self.base_url,
            token=self.token,
            request_timeout=self.request_timeout,
            verify=self.verify,
            auth_error_cls=self.auth_error_cls,
            service_name=self.service_name,
        )
        return self.api_client_cls(
            http_client=HttpClient(
                base_url=config.base_url,
                timeout=config.timeout,
                verify=config.verify,
                api_error_cls=self.api_error_cls,
            ),
            token_provider=StaticTokenProvider(config.token),
        )
