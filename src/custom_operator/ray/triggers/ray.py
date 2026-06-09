"""Deferrable trigger for AiDatalake Ray jobs."""

from __future__ import annotations

from typing import Any

from custom_operator.common.triggers import BaseJobTrigger
from custom_operator.ray.hooks.ray import RayHook
from custom_operator.ray.models.ray import (
    FAILURE_STATES,
    TERMINAL_STATES,
    extract_job_id,
    extract_job_state,
)


class RayJobTrigger(BaseJobTrigger):
    """Poll an AiDatalake Ray job until it reaches a terminal state."""

    service_name = "Ray"
    terminal_states = TERMINAL_STATES
    success_state = "SUCCEEDED"

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
        super().__init__(
            workspace_id=workspace_id,
            job_id=job_id,
            poll_interval=poll_interval,
            max_poll_failures=max_poll_failures,
        )
        self.ray_conn_id = ray_conn_id
        self.ray_base_url = ray_base_url
        self.token = token
        self.request_timeout = request_timeout
        self.verify = verify

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
        return ("custom_operator.ray.triggers.ray.RayJobTrigger", kwargs)

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

    def _on_poll_failures(self) -> None:
        self._cancel_for_cleanup()

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
