"""Deferrable trigger for AiDatalake Spark jobs."""

from __future__ import annotations

import asyncio
from typing import Any

from airflow.triggers.base import BaseTrigger, TriggerEvent

from airflow_provider_aidatalake.hooks.spark import SparkHook
from airflow_provider_aidatalake.models.spark import FAILURE_STATES, TERMINAL_STATES


class SparkJobTrigger(BaseTrigger):
    """Poll an AiDatalake Spark job until it reaches a terminal state."""

    def __init__(
        self,
        *,
        spark_conn_id: str | None,
        auth_conn_id: str | None,
        workspace_id: str,
        job_id: str,
        spark_base_url: str | None = None,
        auth_url: str | None = None,
        auth_body: dict[str, Any] | None = None,
        auth_headers: dict[str, str] | None = None,
        request_timeout: int = 30,
        verify: bool = True,
        poll_interval: int = 30,
        max_poll_failures: int = 10,
        fetch_detail_on_poll: bool = True,
    ) -> None:
        super().__init__()
        self.spark_conn_id = spark_conn_id
        self.auth_conn_id = auth_conn_id
        self.spark_base_url = spark_base_url
        self.auth_url = auth_url
        self.auth_body = dict(auth_body or {})
        self.auth_headers = dict(auth_headers or {})
        self.request_timeout = request_timeout
        self.verify = verify
        self.workspace_id = workspace_id
        self.job_id = job_id
        self.poll_interval = poll_interval
        self.max_poll_failures = max_poll_failures
        self.fetch_detail_on_poll = fetch_detail_on_poll

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow_provider_aidatalake.triggers.spark.SparkJobTrigger",
            {
                "spark_conn_id": self.spark_conn_id,
                "auth_conn_id": self.auth_conn_id,
                "spark_base_url": self.spark_base_url,
                "auth_url": self.auth_url,
                "auth_body": self.auth_body,
                "auth_headers": self.auth_headers,
                "request_timeout": self.request_timeout,
                "verify": self.verify,
                "workspace_id": self.workspace_id,
                "job_id": self.job_id,
                "poll_interval": self.poll_interval,
                "max_poll_failures": self.max_poll_failures,
                "fetch_detail_on_poll": self.fetch_detail_on_poll,
            },
        )

    async def run(self):
        failure_count = 0
        while True:
            try:
                event = await asyncio.to_thread(self._poll_once)
                failure_count = 0
                state = event.get("state")
                if state in TERMINAL_STATES:
                    status = "success" if state == "SUCCEED" else "failed"
                    event["status"] = status
                    yield TriggerEvent(event)
                    return
            except Exception as exc:
                failure_count += 1
                if failure_count >= self.max_poll_failures:
                    yield TriggerEvent(
                        {
                            "status": "failed",
                            "job_id": self.job_id,
                            "message": (
                                "Spark job polling failed "
                                f"{failure_count} consecutive times: {exc}"
                            ),
                        }
                    )
                    return

            await asyncio.sleep(self.poll_interval)

    async def cleanup(self) -> None:
        try:
            await asyncio.to_thread(self._cancel_for_cleanup)
        except Exception as exc:
            self.log.exception("Spark trigger cleanup failed for job_id=%s: %s", self.job_id, exc)

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
        hook = self._hook()
        hook.cancel_job(self.job_id, check_state=True)

    def _hook(self) -> SparkHook:
        return SparkHook(
            spark_conn_id=self.spark_conn_id,
            auth_conn_id=self.auth_conn_id,
            spark_base_url=self.spark_base_url,
            auth_url=self.auth_url,
            auth_body=self.auth_body,
            auth_headers=self.auth_headers,
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
