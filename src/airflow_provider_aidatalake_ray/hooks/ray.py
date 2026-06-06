"""Airflow hook for AiDatalake Ray jobs."""

from __future__ import annotations

from typing import Any

from airflow.sdk import BaseHook

from airflow_provider_aidatalake_ray.clients.http_client import HttpClient
from airflow_provider_aidatalake_ray.clients.ray_api import RayApiClient
from airflow_provider_aidatalake_ray.clients.token import StaticTokenProvider
from airflow_provider_aidatalake_ray.exceptions import AiDatalakeRayApiError, AiDatalakeRayAuthError
from airflow_provider_aidatalake_ray.models.ray import CANCELABLE_STATES, TERMINAL_STATES, extract_job_state


class RayHook(BaseHook):
    """High-level Ray job operations used by operators and triggers."""

    conn_name_attr = "ray_conn_id"
    default_conn_name = "aidatalake_ray"
    conn_type = "http"
    hook_name = "AiDatalake Ray"

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
        super().__init__()
        self.ray_conn_id = ray_conn_id or self.default_conn_name
        self.workspace_id = workspace_id
        self.ray_base_url = ray_base_url
        self.token = token
        self.request_timeout = request_timeout
        self.verify = verify
        self._client: RayApiClient | None = None

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

    @property
    def client(self) -> RayApiClient:
        if self._client is None:
            self._client = self._build_client()
        return self._client

    def _build_client(self) -> RayApiClient:
        if self.ray_base_url and self.token:
            return RayApiClient(
                http_client=HttpClient(
                    base_url=self.ray_base_url,
                    timeout=self.request_timeout,
                    verify=self.verify,
                ),
                token_provider=StaticTokenProvider(self.token),
            )

        conn = self.get_connection(self.ray_conn_id)
        extra = conn.extra_dejson or {}
        timeout = int(extra.get("timeout", self.request_timeout))
        verify = _as_bool(extra.get("verify", self.verify))
        base_url = self.ray_base_url or _connection_base_url(conn)
        token = self.token or extra.get("token") or conn.password
        if not token:
            raise AiDatalakeRayAuthError(
                "Ray token must be configured in DAG token, Connection password, or extra.token"
            )

        return RayApiClient(
            http_client=HttpClient(base_url=base_url, timeout=timeout, verify=verify),
            token_provider=StaticTokenProvider(str(token)),
        )


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
