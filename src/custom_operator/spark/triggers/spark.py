"""Deferrable trigger for AiDatalake Spark jobs."""

from __future__ import annotations

from typing import Any

from custom_operator.common.triggers import BaseJobTrigger
from custom_operator.spark.hooks.spark import SparkHook
from custom_operator.spark.models.spark import FAILURE_STATES, TERMINAL_STATES


class SparkJobTrigger(BaseJobTrigger):
    """Poll an AiDatalake Spark job until it reaches a terminal state."""

    service_name = "Spark"
    terminal_states = TERMINAL_STATES
    success_state = "SUCCEED"

    def __init__(
        self,
        *,
        spark_conn_id: str | None,
        workspace_id: str,
        job_id: str,
        spark_base_url: str | None = None,
        token: str | None = None,
        request_timeout: int = 30,
        verify: bool = True,
        poll_interval: int = 30,
        max_poll_failures: int = 10,
        fetch_detail_on_poll: bool = True,
    ) -> None:
        super().__init__(
            workspace_id=workspace_id,
            job_id=job_id,
            poll_interval=poll_interval,
            max_poll_failures=max_poll_failures,
        )
        self.spark_conn_id = spark_conn_id
        self.spark_base_url = spark_base_url
        self.token = token
        self.request_timeout = request_timeout
        self.verify = verify
        self.fetch_detail_on_poll = fetch_detail_on_poll

    def serialize(self) -> tuple[str, dict[str, Any]]:
        kwargs = {
            "spark_conn_id": self.spark_conn_id,
            "spark_base_url": self.spark_base_url,
            "request_timeout": self.request_timeout,
            "verify": self.verify,
            "workspace_id": self.workspace_id,
            "job_id": self.job_id,
            "poll_interval": self.poll_interval,
            "max_poll_failures": self.max_poll_failures,
            "fetch_detail_on_poll": self.fetch_detail_on_poll,
        }
        if self.token:
            kwargs["token"] = self.token
        return ("custom_operator.spark.triggers.spark.SparkJobTrigger", kwargs)

    def _poll_once(self) -> dict[str, Any]:
        hook = self._hook()
        state_response = hook.get_job_state(self.job_id)
        state = state_response.get("state")
        event: dict[str, Any] = {
            "status": "running",
            "job_id": state_response.get("job_id", self.job_id),
            "state": state,
        }

        if self.fetch_detail_on_poll:
            try:
                detail = hook.get_job_detail(self.job_id)
                event["log_url"] = detail.get("log_url")
                if state in TERMINAL_STATES:
                    event["detail"] = _compact_detail(detail)
            except Exception as exc:
                event["detail_error"] = str(exc)

        if state in FAILURE_STATES:
            event.setdefault("message", f"Spark job finished with state {state}")
        return event

    def _cancel_for_cleanup(self) -> None:
        self.log.info("Spark trigger cleanup cancelling job_id=%s", self.job_id)
        hook = self._hook()
        hook.cancel_job(self.job_id, check_state=True)

    def _hook(self) -> SparkHook:
        return SparkHook(
            spark_conn_id=self.spark_conn_id,
            spark_base_url=self.spark_base_url,
            token=self.token,
            request_timeout=self.request_timeout,
            verify=self.verify,
            workspace_id=self.workspace_id,
        )


def _compact_detail(detail: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "job_id",
        "state",
        "retry_times",
        "create_time",
        "start_time",
        "end_time",
        "log_url",
    )
    return {key: detail.get(key) for key in keys if key in detail}
