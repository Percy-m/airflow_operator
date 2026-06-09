"""Airflow hook for AiDatalake Ray jobs."""

from __future__ import annotations

from typing import Any

from custom_operator.common.hooks import BaseComputeHook
from custom_operator.ray.clients.ray_api import RayApiClient
from custom_operator.ray.exceptions import AiDatalakeRayApiError, AiDatalakeRayAuthError
from custom_operator.ray.models.ray import CANCELABLE_STATES, TERMINAL_STATES, extract_job_state


class RayHook(BaseComputeHook):
    """High-level Ray job operations used by operators and triggers."""

    conn_name_attr = "ray_conn_id"
    default_conn_name = "aidatalake_ray"
    conn_type = "http"
    hook_name = "AiDatalake Ray"
    api_client_cls = RayApiClient
    api_error_cls = AiDatalakeRayApiError
    auth_error_cls = AiDatalakeRayAuthError
    service_name = "Ray"

    def __init__(
        self,
        *,
        ray_conn_id: str | None = None,
        workspace_id: str,
        ray_base_url: str | None = None,
        token: str | None = None,
        request_timeout: int = 30,
        verify: bool = True,
    ) -> None:
        resolved_conn_id = ray_conn_id or self.default_conn_name
        super().__init__(
            conn_id=resolved_conn_id,
            workspace_id=workspace_id,
            base_url=ray_base_url,
            token=token,
            request_timeout=request_timeout,
            verify=verify,
        )
        self.ray_conn_id = resolved_conn_id
        self.ray_base_url = ray_base_url

    def submit_job(self, payload: dict[str, Any], *, transaction_id: str | None = None) -> str:
        return self.client.create_job(
            workspace_id=self.workspace_id,
            payload=payload,
            transaction_id=transaction_id,
        )

    def get_job_detail(self, job_id: str) -> dict[str, Any]:
        return self.client.get_job_detail(workspace_id=self.workspace_id, job_id=job_id)

    def get_job_state(self, job_id: str) -> dict[str, Any]:
        detail = self.get_job_detail(job_id)
        state = extract_job_state(detail)
        if not state:
            raise AiDatalakeRayApiError("Ray job detail response does not contain state")
        return {"job_id": job_id, "state": state, "detail": detail}

    def cancel_job(self, job_id: str, *, check_state: bool = True) -> bool:
        if check_state:
            try:
                state = self.get_job_state(job_id).get("state")
                if state in TERMINAL_STATES:
                    self.log.info("Skip Ray job cancel because job is already terminal: %s", state)
                    return False
                if state and state not in CANCELABLE_STATES:
                    self.log.info("Skip Ray job cancel because state is not cancelable: %s", state)
                    return False
            except Exception as exc:
                self.log.warning("Could not query Ray job state before cancel, trying cancel: %s", exc)

        self.client.cancel_job(workspace_id=self.workspace_id, job_id=job_id)
        return True
