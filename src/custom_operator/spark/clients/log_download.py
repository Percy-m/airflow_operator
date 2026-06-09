"""Client for WorkspaceCore Spark log download APIs."""

from __future__ import annotations

import logging
from typing import Any

from custom_operator.spark.exceptions import AiDatalakeApiError
from custom_operator.spark.http_client import HttpClient
from custom_operator.spark.token import mask_token

log = logging.getLogger(__name__)

LOG_DOWNLOAD_STATUS_KEY = "spark_log_download_status"
LOG_DOWNLOAD_URL_KEY = "spark_log_download_url"
LOG_DOWNLOAD_EXPIRES_AT_KEY = "spark_log_download_expires_at"
LOG_DOWNLOAD_FILE_SIZE_KEY = "spark_log_file_size"
LOG_DOWNLOAD_FILE_EXISTS_KEY = "spark_log_file_exists"
LOG_DOWNLOAD_MESSAGE_KEY = "spark_log_download_message"
LOG_DOWNLOAD_ERROR_CODE_KEY = "spark_log_download_error_code"

LOG_DOWNLOAD_EVENT_KEYS = (
    LOG_DOWNLOAD_STATUS_KEY,
    LOG_DOWNLOAD_URL_KEY,
    LOG_DOWNLOAD_EXPIRES_AT_KEY,
    LOG_DOWNLOAD_FILE_SIZE_KEY,
    LOG_DOWNLOAD_FILE_EXISTS_KEY,
    LOG_DOWNLOAD_MESSAGE_KEY,
    LOG_DOWNLOAD_ERROR_CODE_KEY,
)

_STATUS_BY_ERROR_CODE = {
    "AIPDWS.BIZ.2050001": "pending",
    "AIPDWS.BIZ.2050002": "too_large",
}


class SparkLogDownloadClient:
    """Calls WorkspaceCoreService to generate Spark log download URLs."""

    def __init__(self, *, http_client: HttpClient, internal_auth_token: str) -> None:
        self.http_client = http_client
        self.internal_auth_token = internal_auth_token

    def create_download_url(self, *, job_id: str, log_path: str) -> dict[str, Any]:
        headers = self._auth_headers()
        payload = {
            "jobId": job_id,
            "logPath": log_path,
        }
        log.info(
            "Creating Spark log download URL path=%s headers=%s request_body=%s",
            "/internal/log/v1/create",
            _mask_headers(headers),
            payload,
        )
        response = self.http_client.request(
            "POST",
            "/internal/log/v1/create",
            headers=headers,
            json=payload,
            expected_statuses={200},
            retry=0,
        )
        try:
            body = response.json()
        except ValueError as exc:
            raise AiDatalakeApiError("Spark log download response must be JSON") from exc
        if not isinstance(body, dict):
            raise AiDatalakeApiError("Spark log download response must be a JSON object")
        return _normalize_response(body)

    def _auth_headers(self) -> dict[str, str]:
        return {
            "X-Internal-Auth-Token": self.internal_auth_token,
            "Content-Type": "application/json",
        }


def _normalize_response(body: dict[str, Any]) -> dict[str, Any]:
    status = body.get("status")
    if status == "OK":
        data = body.get("data")
        if not isinstance(data, dict):
            raise AiDatalakeApiError("Spark log download response does not contain data")
        download_url = data.get("downloadUrl")
        if not download_url:
            raise AiDatalakeApiError("Spark log download response does not contain downloadUrl")
        return {
            LOG_DOWNLOAD_STATUS_KEY: "available",
            LOG_DOWNLOAD_URL_KEY: download_url,
            LOG_DOWNLOAD_EXPIRES_AT_KEY: data.get("expiresAt"),
            LOG_DOWNLOAD_FILE_SIZE_KEY: data.get("fileSize"),
            LOG_DOWNLOAD_FILE_EXISTS_KEY: data.get("fileExists"),
        }

    error_code = body.get("errorCode") or body.get("error_code")
    error_msg = body.get("errorMsg") or body.get("error_msg") or "Spark log download failed"
    return {
        LOG_DOWNLOAD_STATUS_KEY: _STATUS_BY_ERROR_CODE.get(str(error_code), "error"),
        LOG_DOWNLOAD_MESSAGE_KEY: error_msg,
        LOG_DOWNLOAD_ERROR_CODE_KEY: error_code,
    }


def _mask_headers(headers: dict[str, str]) -> dict[str, str]:
    masked: dict[str, str] = {}
    for key, value in headers.items():
        if "token" in key.lower() or "authorization" in key.lower():
            masked[key] = mask_token(value)
        else:
            masked[key] = value
    return masked
