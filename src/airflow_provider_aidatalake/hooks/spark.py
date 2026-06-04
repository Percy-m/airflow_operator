"""Airflow hook for AiDatalake Spark jobs."""

from __future__ import annotations

from typing import Any

from airflow.sdk import BaseHook

from airflow_provider_aidatalake.clients.http import HttpClient
from airflow_provider_aidatalake.clients.spark_api import SparkApiClient
from airflow_provider_aidatalake.clients.token import TokenProvider
from airflow_provider_aidatalake.exceptions import AiDatalakeAuthError
from airflow_provider_aidatalake.models.spark import CANCELABLE_STATES, TERMINAL_STATES


class SparkHook(BaseHook):
    """High-level Spark job operations used by operators and triggers."""

    conn_name_attr = "spark_conn_id"
    default_conn_name = "aidatalake_spark"
    conn_type = "http"
    hook_name = "AiDatalake Spark"

    def __init__(
        self,
        *,
        spark_conn_id: str = default_conn_name,
        auth_conn_id: str | None = None,
        workspace_id: str,
    ) -> None:
        super().__init__()
        self.spark_conn_id = spark_conn_id
        self.auth_conn_id = auth_conn_id or spark_conn_id
        self.workspace_id = workspace_id
        self._client: SparkApiClient | None = None

    def submit_job(self, payload: dict[str, Any], *, client_token: str | None = None) -> str:
        return self.client.create_job(
            workspace_id=self.workspace_id,
            payload=payload,
            client_token=client_token,
        )

    def get_job_state(self, job_id: str) -> dict[str, Any]:
        return self.client.get_job_state(workspace_id=self.workspace_id, job_id=job_id)

    def get_job_detail(self, job_id: str) -> dict[str, Any]:
        return self.client.get_job_detail(workspace_id=self.workspace_id, job_id=job_id)

    def cancel_job(self, job_id: str, *, check_state: bool = True) -> bool:
        if check_state:
            try:
                state = self.get_job_state(job_id).get("state")
                if state in TERMINAL_STATES or state == "CANCELING":
                    self.log.info("Skip Spark job cancel because job is already terminal: %s", state)
                    return False
                if state and state not in CANCELABLE_STATES:
                    self.log.info("Skip Spark job cancel because state is not cancelable: %s", state)
                    return False
            except Exception as exc:
                self.log.warning("Could not query Spark job state before cancel, trying cancel: %s", exc)

        self.client.cancel_job(workspace_id=self.workspace_id, job_id=job_id)
        return True

    @property
    def client(self) -> SparkApiClient:
        if self._client is None:
            self._client = self._build_client()
        return self._client

    def _build_client(self) -> SparkApiClient:
        spark_conn = self.get_connection(self.spark_conn_id)
        auth_conn = self.get_connection(self.auth_conn_id)

        spark_extra = spark_conn.extra_dejson or {}
        auth_extra = auth_conn.extra_dejson or {}
        timeout = int(spark_extra.get("timeout", auth_extra.get("timeout", 30)))
        verify = _as_bool(spark_extra.get("verify", auth_extra.get("verify", True)))

        spark_base_url = _connection_base_url(spark_conn)
        auth_base_url = _connection_base_url(auth_conn)
        auth_url = auth_extra.get("auth_url") or spark_extra.get("auth_url")
        if not auth_url:
            raise AiDatalakeAuthError("auth_url must be configured in Connection extra")

        auth_body = auth_extra.get("auth_body", spark_extra.get("auth_body", {}))
        auth_headers = auth_extra.get("auth_headers", spark_extra.get("auth_headers", {}))

        spark_http_client = HttpClient(base_url=spark_base_url, timeout=timeout, verify=verify)
        auth_http_client = HttpClient(base_url=auth_base_url, timeout=timeout, verify=verify)
        token_provider = TokenProvider(
            http_client=auth_http_client,
            auth_url=auth_url,
            auth_body=auth_body,
            auth_headers=auth_headers,
        )
        return SparkApiClient(http_client=spark_http_client, token_provider=token_provider)


def _connection_base_url(conn) -> str:
    if conn.host and conn.host.startswith(("http://", "https://")):
        base_url = conn.host
    else:
        schema = conn.schema or "https"
        host = conn.host or ""
        base_url = f"{schema}://{host}"
    if conn.port:
        base_url = f"{base_url}:{conn.port}"
    return base_url.rstrip("/")


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() not in {"0", "false", "no", "off"}
    return bool(value)
