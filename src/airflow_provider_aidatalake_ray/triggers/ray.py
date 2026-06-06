"""Deferrable trigger for AiDatalake Ray jobs."""

from __future__ import annotations

import asyncio
from typing import Any

from airflow.triggers.base import BaseTrigger, TriggerEvent

from airflow_provider_aidatalake_ray.hooks.ray import RayHook
from airflow_provider_aidatalake_ray.models.ray import (
    FAILURE_STATES,
    TERMINAL_STATES,
    extract_job_id,
    extract_job_state,
)


class RayJobTrigger(BaseTrigger):
    """Poll an AiDatalake Ray job until it reaches a terminal state."""

    def __init__(
        self,
        *,
        ray_conn_id: str | None,
        workspace_id: str,
        job_id: str,
        ray_base_url: str | None = None,
        token: str | None = None,
        request_timeout: int = 30,
        verify: bool = True,
        poll_interval: int = 30,
        max_poll_failures: int = 10,
    ) -> None:
        super().__init__()
        self.ray_conn_id = ray_conn_id
        self.ray_base_url = ray_base_url
        self.token = token
        self.request_timeout = request_timeout
        self.verify = verify
        self.workspace_id = workspace_id
        self.job_id = job_id
        self.poll_interval = poll_interval
        self.max_poll_failures = max_poll_failures

    def serialize(self) -> tuple[str, dict[str, Any]]:
        kwargs = {
            "ray_conn_id": self.ray_conn_id,
            "ray_base_url": self.ray_base_url,
            "request_timeout": self.request_timeout,
            "verify": self.verify,
            "workspace_id": self.workspace_id,
            "job_id": self.job_id,
            "poll_interval": self.poll_interval,
            "max_poll_failures": self.max_poll_failures,
        }
        if self.token:
            kwargs["token"] = self.token
        return ("airflow_provider_aidatalake_ray.triggers.ray.RayJobTrigger", kwargs)

    async def run(self):
        self.log.info(
            "Ray trigger started job_id=%s poll_interval=%s",
            self.job_id,
            self.poll_interval,
        )
        failure_count = 0
        while True:
            try:
                event = await asyncio.to_thread(self._poll_once)
                failure_count = 0
                state = event.get("state")
                self.log.info("Ray trigger poll succeeded job_id=%s state=%s", self.job_id, state)
                if state in TERMINAL_STATES:
                    event["status"] = "success" if state == "SUCCEEDED" else "failed"
                    yield TriggerEvent(event)
                    return
            except Exception as exc:
                failure_count += 1
                self.log.warning(
                    "Ray trigger poll failed job_id=%s failure_count=%s: %s",
                    self.job_id,
                    failure_count,
                    exc,
                )
                if failure_count >= self.max_poll_failures:
                    await asyncio.to_thread(self._cancel_for_cleanup)
                    yield TriggerEvent(
                        {
                            "status": "failed",
                            "job_id": self.job_id,
                            "message": (
                                "Ray job polling failed "
                                f"{failure_count} consecutive times: {exc}"
                            ),
                        }
                    )
                    return

            await asyncio.sleep(self.poll_interval)

    async def cleanup(self) -> None:
        self.log.info("Ray trigger cleanup started job_id=%s", self.job_id)
        try:
            await asyncio.to_thread(self._cancel_for_cleanup)
        except Exception as exc:
            self.log.exception("Ray trigger cleanup failed for job_id=%s: %s", self.job_id, exc)

    def _poll_once(self) -> dict[str, Any]:
        hook = self._hook()
        detail = hook.get_job_detail(self.job_id)
        state = extract_job_state(detail)
        if not state:
            raise ValueError("Ray job detail response does not contain state")
        event: dict[str, Any] = {
            "status": "running",
            "job_id": extract_job_id(detail, self.job_id),
            "state": state,
        }
        if state in TERMINAL_STATES:
            event["detail"] = _compact_detail(detail)
        if state in FAILURE_STATES:
            event["message"] = f"Ray job finished with state {state}"
        return event

    def _cancel_for_cleanup(self) -> None:
        self.log.info("Ray trigger cleanup cancelling job_id=%s", self.job_id)
        self._hook().cancel_job(self.job_id, check_state=False)

    def _hook(self) -> RayHook:
        return RayHook(
            ray_conn_id=self.ray_conn_id,
            ray_base_url=self.ray_base_url,
            token=self.token,
            request_timeout=self.request_timeout,
            verify=self.verify,
            workspace_id=self.workspace_id,
        )


def _compact_detail(detail: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "id",
        "job_id",
        "name",
        "state",
        "status",
        "endpoint_name",
        "create_time",
        "start_time",
        "end_time",
    )
    return {key: detail.get(key) for key in keys if key in detail}
