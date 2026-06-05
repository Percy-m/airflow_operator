"""Client for AiDatalake Spark job APIs."""

from __future__ import annotations

import logging
from typing import Any
from uuid import uuid4

from airflow_provider_aidatalake.clients.http import HttpClient
from airflow_provider_aidatalake.clients.token import TokenProvider, mask_token
from airflow_provider_aidatalake.exceptions import AiDatalakeApiError

log = logging.getLogger(__name__)


class SparkApiClient:
    """Calls Spark job APIs from AiDatalake API specification."""

    def __init__(self, *, http_client: HttpClient, token_provider: TokenProvider) -> None:
        self.http_client = http_client
        self.token_provider = token_provider

    def create_job(
        self,
        *,
        workspace_id: str,
        payload: dict[str, Any],
        client_token: str | None = None,
    ) -> str:
        headers = self._auth_headers()
        headers["X-Client-Token"] = client_token or str(uuid4())
        path = f"/v2/workspaces/{workspace_id}/spark-jobs"
        log.info(
            "Creating Spark job request path=%s headers=%s request_body=%s",
            path,
            _mask_headers(headers),
            payload,
        )
        response = self._request_with_token_refresh(
            "POST",
            path,
            headers=headers,
            json=payload,
            expected_statuses={201},
            retry=0,
        )
        body = response.json()
        job_id = body.get("job_id")
        if not job_id:
            raise AiDatalakeApiError("Spark create job response does not contain job_id")
        return job_id

    def get_job_state(self, *, workspace_id: str, job_id: str) -> dict[str, Any]:
        response = self._request_with_token_refresh(
            "GET",
            f"/v2/workspaces/{workspace_id}/spark-jobs/{job_id}/state",
            headers=self._auth_headers(),
            expected_statuses={200},
            retry=0,
        )
        return response.json()

    def get_job_detail(self, *, workspace_id: str, job_id: str) -> dict[str, Any]:
        response = self._request_with_token_refresh(
            "GET",
            f"/v2/workspaces/{workspace_id}/spark-jobs/{job_id}",
            headers=self._auth_headers(),
            expected_statuses={200},
            retry=0,
        )
        return response.json()

    def cancel_job(self, *, workspace_id: str, job_id: str) -> None:
        self._request_with_token_refresh(
            "POST",
            f"/v2/workspaces/{workspace_id}/spark-jobs/{job_id}/cancel",
            headers=self._auth_headers(),
            expected_statuses={204},
            retry=0,
        )

    def _auth_headers(self) -> dict[str, str]:
        return {
            "X-Auth-Token": self.token_provider.get_token(),
            "Content-Type": "application/json",
        }

    def _request_with_token_refresh(self, method: str, path: str, **kwargs: Any):
        try:
            return self.http_client.request(method, path, **kwargs)
        except AiDatalakeApiError as exc:
            if exc.status_code != 401:
                raise
            headers = dict(kwargs.get("headers") or {})
            headers["X-Auth-Token"] = self.token_provider.refresh_token()
            kwargs["headers"] = headers
            return self.http_client.request(method, path, **kwargs)


def _mask_headers(headers: dict[str, str]) -> dict[str, str]:
    masked: dict[str, str] = {}
    for key, value in headers.items():
        if "token" in key.lower() or "authorization" in key.lower():
            masked[key] = mask_token(value)
        else:
            masked[key] = value
    return masked
