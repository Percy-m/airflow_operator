"""Airflow hook for AiDatalake Spark jobs."""

from __future__ import annotations

from typing import Any

from custom_operator.common.hooks import BaseComputeHook
from custom_operator.spark.clients.spark_api import SparkApiClient
from custom_operator.spark.exceptions import AiDatalakeApiError, AiDatalakeAuthError
from custom_operator.spark.models.spark import CANCELABLE_STATES, TERMINAL_STATES


class SparkHook(BaseComputeHook):
    """High-level Spark job operations used by operators and triggers."""

    conn_name_attr = "spark_conn_id"
    default_conn_name = "aidatalake_spark"
    conn_type = "http"
    hook_name = "AiDatalake Spark"
    api_client_cls = SparkApiClient
    api_error_cls = AiDatalakeApiError
    auth_error_cls = AiDatalakeAuthError
    service_name = "Spark"

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
        resolved_conn_id = spark_conn_id or self.default_conn_name
        super().__init__(
            conn_id=resolved_conn_id,
            workspace_id=workspace_id,
            base_url=spark_base_url,
            token=token,
            request_timeout=request_timeout,
            verify=verify,
        )
        self.spark_conn_id = resolved_conn_id
        self.spark_base_url = spark_base_url

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
