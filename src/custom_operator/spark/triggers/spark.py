"""Deferrable trigger for AiDatalake Spark jobs."""

from __future__ import annotations

import asyncio
from typing import Any

from airflow.triggers.base import BaseTrigger, TriggerEvent

from custom_operator.spark.hooks.log_download import SparkLogDownloadHook
from custom_operator.spark.hooks.spark import SparkHook
from custom_operator.spark.models.spark import FAILURE_STATES, TERMINAL_STATES


class SparkJobTrigger(BaseTrigger):
    """Poll an AiDatalake Spark job until it reaches a terminal state."""

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
        workspace_core_conn_id: str | None = None,
        workspace_core_base_url: str | None = None,
        workspace_core_internal_token: str | None = None,
        log_download_timeout: int = 5,
        enable_log_download: bool = True,
        poll_interval: int = 30,
        max_poll_failures: int = 10,
        fetch_detail_on_poll: bool = True,
    ) -> None:
        super().__init__()
        self.spark_conn_id = spark_conn_id
        self.spark_base_url = spark_base_url
        self.token = token
        self.request_timeout = request_timeout
        self.verify = verify
        self.workspace_core_conn_id = workspace_core_conn_id
        self.workspace_core_base_url = workspace_core_base_url
        self.workspace_core_internal_token = workspace_core_internal_token
        self.log_download_timeout = log_download_timeout
        self.enable_log_download = enable_log_download
        self.workspace_id = workspace_id
        self.job_id = job_id
        self.poll_interval = poll_interval
        self.max_poll_failures = max_poll_failures
        self.fetch_detail_on_poll = fetch_detail_on_poll
        self._log_download_result: dict[str, Any] | None = None

    def serialize(self) -> tuple[str, dict[str, Any]]:
        kwargs = {
            "spark_conn_id": self.spark_conn_id,
            "spark_base_url": self.spark_base_url,
            "request_timeout": self.request_timeout,
            "verify": self.verify,
            "workspace_core_conn_id": self.workspace_core_conn_id,
            "workspace_core_base_url": self.workspace_core_base_url,
            "log_download_timeout": self.log_download_timeout,
            "enable_log_download": self.enable_log_download,
            "workspace_id": self.workspace_id,
            "job_id": self.job_id,
            "poll_interval": self.poll_interval,
            "max_poll_failures": self.max_poll_failures,
            "fetch_detail_on_poll": self.fetch_detail_on_poll,
        }
        if self.token:
            kwargs["token"] = self.token
        if self.workspace_core_internal_token:
            kwargs["workspace_core_internal_token"] = self.workspace_core_internal_token
        return ("custom_operator.spark.triggers.spark.SparkJobTrigger", kwargs)

    async def run(self):
        self.log.info(
            "Spark trigger started job_id=%s poll_interval=%s",
            self.job_id,
            self.poll_interval,
        )
        failure_count = 0
        while True:
            try:
                event = await asyncio.to_thread(self._poll_once)
                failure_count = 0
                state = event.get("state")
                self.log.info("Spark trigger poll succeeded job_id=%s state=%s", self.job_id, state)
                if state in TERMINAL_STATES:
                    status = "success" if state == "SUCCEED" else "failed"
                    event["status"] = status
                    yield TriggerEvent(event)
                    return
            except Exception as exc:
                failure_count += 1
                self.log.warning(
                    "Spark trigger poll failed job_id=%s failure_count=%s: %s",
                    self.job_id,
                    failure_count,
                    exc,
                )
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
        self.log.info("Spark trigger cleanup started job_id=%s", self.job_id)
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
                log_url = detail.get("log_url")
                event["log_url"] = log_url
                if log_url:
                    self._log_download_result = self._create_log_download_result(log_url)
                if state in TERMINAL_STATES:
                    event["detail"] = _compact_detail(detail)
            except Exception as exc:
                event["detail_error"] = str(exc)

        if self._log_download_result:
            event.update(self._log_download_result)

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

    def _log_download_hook(self) -> SparkLogDownloadHook:
        return SparkLogDownloadHook(
            workspace_core_conn_id=self.workspace_core_conn_id,
            workspace_core_base_url=self.workspace_core_base_url,
            workspace_core_internal_token=self.workspace_core_internal_token,
            request_timeout=self.log_download_timeout,
            verify=self.verify,
        )

    def _create_log_download_result(self, log_url: str) -> dict[str, Any]:
        if not self.enable_log_download:
            return {
                "spark_log_download_status": "disabled",
                "spark_log_download_message": "Spark log download is disabled",
            }
        if not self._has_log_download_config():
            self.log.warning(
                "Skip Spark log download URL creation because WorkspaceCoreService is not configured"
            )
            return {
                "spark_log_download_status": "disabled",
                "spark_log_download_message": "WorkspaceCoreService log download is not configured",
            }

        try:
            result = self._log_download_hook().create_download_url(
                job_id=self.job_id,
                log_path=log_url,
            )
            status = result.get("spark_log_download_status")
            if status == "available":
                self.log.info("Spark log download URL created job_id=%s", self.job_id)
            else:
                self.log.warning(
                    "Spark log download URL not available job_id=%s status=%s message=%s",
                    self.job_id,
                    status,
                    result.get("spark_log_download_message"),
                )
            return result
        except Exception as exc:
            self.log.warning(
                "Failed to create Spark log download URL job_id=%s: %s",
                self.job_id,
                exc,
            )
            return {
                "spark_log_download_status": "error",
                "spark_log_download_message": str(exc),
            }

    def _has_log_download_config(self) -> bool:
        return bool(
            self.workspace_core_conn_id
            or self.workspace_core_base_url
            or self.workspace_core_internal_token
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
