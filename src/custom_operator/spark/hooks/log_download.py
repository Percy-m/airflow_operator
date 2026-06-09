"""Airflow hook for WorkspaceCore Spark log download integration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from airflow.sdk import BaseHook

from custom_operator.spark.clients.log_download import SparkLogDownloadClient
from custom_operator.spark.connection import connection_base_url
from custom_operator.spark.exceptions import AiDatalakeAuthError
from custom_operator.spark.http_client import HttpClient


@dataclass(frozen=True)
class WorkspaceCoreConnectionConfig:
    base_url: str
    internal_auth_token: str
    timeout: int
    verify: bool


class ConnectionReader(Protocol):
    def get_connection(self, conn_id: str): ...


class SparkLogDownloadHook(BaseHook):
    """Create Spark log download URLs through WorkspaceCoreService."""

    conn_name_attr = "workspace_core_conn_id"
    default_conn_name = "aidatalake_workspace_core"
    conn_type = "http"
    hook_name = "AiDatalake Workspace Core"

    def __init__(
        self,
        *,
        workspace_core_conn_id: str | None = None,
        workspace_core_base_url: str | None = None,
        workspace_core_internal_token: str | None = None,
        request_timeout: int = 5,
        verify: bool = True,
    ) -> None:
        super().__init__()
        self.workspace_core_conn_id = workspace_core_conn_id
        self.workspace_core_base_url = workspace_core_base_url
        self.workspace_core_internal_token = workspace_core_internal_token
        self.request_timeout = request_timeout
        self.verify = verify
        self._client: SparkLogDownloadClient | None = None

    def create_download_url(self, *, job_id: str, log_path: str) -> dict:
        return self.client.create_download_url(job_id=job_id, log_path=log_path)

    @property
    def client(self) -> SparkLogDownloadClient:
        if self._client is None:
            self._client = self._build_client()
        return self._client

    def _build_client(self) -> SparkLogDownloadClient:
        config = resolve_workspace_core_config(
            self,
            conn_id=self.workspace_core_conn_id,
            base_url=self.workspace_core_base_url,
            internal_auth_token=self.workspace_core_internal_token,
            request_timeout=self.request_timeout,
            verify=self.verify,
        )
        return SparkLogDownloadClient(
            http_client=HttpClient(
                base_url=config.base_url,
                timeout=config.timeout,
                verify=config.verify,
            ),
            internal_auth_token=config.internal_auth_token,
        )


def resolve_workspace_core_config(
    hook: ConnectionReader,
    *,
    conn_id: str | None,
    base_url: str | None,
    internal_auth_token: str | None,
    request_timeout: int,
    verify: bool,
) -> WorkspaceCoreConnectionConfig:
    """Resolve WorkspaceCoreService endpoint and internal auth token."""

    if base_url and internal_auth_token:
        return WorkspaceCoreConnectionConfig(
            base_url=base_url.rstrip("/"),
            internal_auth_token=internal_auth_token,
            timeout=request_timeout,
            verify=verify,
        )

    if not conn_id:
        raise AiDatalakeAuthError(
            "WorkspaceCore log download requires explicit base URL/token or workspace_core_conn_id"
        )

    conn = hook.get_connection(conn_id)
    resolved_base_url = base_url or connection_base_url(conn)
    if resolved_base_url in {"http://", "https://"}:
        raise AiDatalakeAuthError("WorkspaceCore base URL must be configured")

    resolved_token = internal_auth_token or conn.password
    if not resolved_token:
        raise AiDatalakeAuthError("WorkspaceCore internal auth token must be configured")

    return WorkspaceCoreConnectionConfig(
        base_url=resolved_base_url.rstrip("/"),
        internal_auth_token=str(resolved_token),
        timeout=request_timeout,
        verify=verify,
    )
