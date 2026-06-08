"""Airflow hook for AiDatalake Spark jobs."""

from __future__ import annotations

from typing import Any

from airflow.sdk import BaseHook

from custom_operator.spark.clients.spark_api import SparkApiClient
from custom_operator.spark.models.spark import CANCELABLE_STATES, TERMINAL_STATES
from custom_operator.token.connection import resolve_connection_config
from custom_operator.token.http_client import HttpClient
from custom_operator.token.token import StaticTokenProvider


class SparkHook(BaseHook):
    """High-level Spark job operations used by operators and triggers."""

    conn_name_attr = "spark_conn_id"
    default_conn_name = "aidatalake_spark"
    conn_type = "http"
    hook_name = "AiDatalake Spark"

    def __init__(
        self,
        *,
        spark_conn_id: str | None = None,
        workspace_id: str,
        spark_base_url: str | None = None,
        token: str | None = None,
        request_timeout: int = 30,
        verify: bool = True,
    ) -> None:
        super().__init__()
        self.spark_conn_id = spark_conn_id or self.default_conn_name
        self.workspace_id = workspace_id
        self.spark_base_url = spark_base_url
        self.token = token
        self.request_timeout = request_timeout
        self.verify = verify
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
        config = resolve_connection_config(
            self,
            conn_id=self.spark_conn_id,
            base_url=self.spark_base_url,
            token=self.token,
            request_timeout=self.request_timeout,
            verify=self.verify,
            service_name="Spark",
        )
        return SparkApiClient(
            http_client=HttpClient(
                base_url=config.base_url,
                timeout=config.timeout,
                verify=config.verify,
            ),
            token_provider=StaticTokenProvider(config.token),
        )
