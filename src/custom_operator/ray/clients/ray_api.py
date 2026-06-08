"""Client for AiDatalake Ray job APIs."""

from __future__ import annotations

import logging
from typing import Any
from uuid import uuid4

from custom_operator.ray.exceptions import AiDatalakeRayApiError
from custom_operator.ray.http_client import HttpClient
from custom_operator.ray.token import StaticTokenProvider, mask_token

log = logging.getLogger(__name__)


class RayApiClient:
    """Calls Ray job APIs from AiDatalake API specification."""

    def __init__(self, *, http_client: HttpClient, token_provider: StaticTokenProvider) -> None:
        self.http_client = http_client
        self.token_provider = token_provider

    def create_job(
        self,
        *,
        workspace_id: str,
        payload: dict[str, Any],
        transaction_id: str | None = None,
    ) -> str:
        headers = self._auth_headers()
        headers["X-Transaction-ID"] = transaction_id or str(uuid4())
        path = f"/v2/workspaces/{workspace_id}/ray-jobs"
        log.info(
            "Creating Ray job request path=%s headers=%s request_body=%s",
            path,
            _mask_headers(headers),
            payload,
        )
        response = self.http_client.request(
            "POST",
            path,
            headers=headers,
            json=payload,
            expected_statuses={202},
            retry=3,
        )
        body = response.json()
        job_id = body.get("id") or body.get("job_id")
        if not job_id:
            raise AiDatalakeRayApiError("Ray create job response does not contain id")
        return job_id

    def get_job_detail(self, *, workspace_id: str, job_id: str) -> dict[str, Any]:
        response = self.http_client.request(
            "GET",
            f"/v2/workspaces/{workspace_id}/ray-jobs/{job_id}",
            headers=self._auth_headers(),
            expected_statuses={200},
            retry=0,
        )
        body = response.json()
        if not isinstance(body, dict):
            raise AiDatalakeRayApiError("Ray job detail response must be a JSON object")
        return body

    def cancel_job(self, *, workspace_id: str, job_id: str) -> None:
        self.http_client.request(
            "POST",
            f"/v2/workspaces/{workspace_id}/ray-jobs/{job_id}/cancel",
            headers=self._auth_headers(),
            expected_statuses={202},
            retry=2,
        )

    def _auth_headers(self) -> dict[str, str]:
        return {
            "X-Auth-Token": self.token_provider.get_token(),
            "Content-Type": "application/json",
        }


def _mask_headers(headers: dict[str, str]) -> dict[str, str]:
    masked: dict[str, str] = {}
    for key, value in headers.items():
        if "token" in key.lower() or "authorization" in key.lower():
            masked[key] = mask_token(value)
        else:
            masked[key] = value
    return masked
