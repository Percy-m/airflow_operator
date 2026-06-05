"""Small requests wrapper with retry and Spark API error mapping."""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping
from typing import Any

import requests

from airflow_provider_aidatalake.exceptions import AiDatalakeApiError

log = logging.getLogger(__name__)


class HttpClient:
    """Synchronous HTTP client used by operator and hook code."""

    def __init__(
        self,
        *,
        base_url: str = "",
        timeout: int = 30,
        verify: bool = True,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.verify = verify
        self.session = session or requests.Session()

    def request(
        self,
        method: str,
        path_or_url: str,
        *,
        headers: Mapping[str, str] | None = None,
        json: Any | None = None,
        expected_statuses: set[int] | None = None,
        retry: int = 0,
        retry_backoff: tuple[float, ...] = (1.0, 2.0, 4.0),
    ) -> requests.Response:
        expected = expected_statuses or {200}
        url = self._build_url(path_or_url)
        attempts = retry + 1
        last_error: Exception | None = None

        for attempt in range(attempts):
            attempt_number = attempt + 1
            log.info(
                "HTTP request started method=%s url=%s attempt=%s timeout=%s",
                method,
                url,
                attempt_number,
                self.timeout,
            )
            start_time = time.perf_counter()
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    headers=dict(headers or {}),
                    json=json,
                    timeout=self.timeout,
                    verify=self.verify,
                )
                cost_ms = int((time.perf_counter() - start_time) * 1000)
                log.info(
                    "HTTP response received method=%s url=%s status_code=%s cost_ms=%s",
                    method,
                    url,
                    response.status_code,
                    cost_ms,
                )
                if response.status_code in expected:
                    return response
                if not self._should_retry_status(response.status_code) or attempt == attempts - 1:
                    self._raise_api_error(response)
            except (requests.Timeout, requests.ConnectionError) as exc:
                cost_ms = int((time.perf_counter() - start_time) * 1000)
                last_error = exc
                log.warning(
                    "HTTP request failed method=%s url=%s attempt=%s cost_ms=%s error=%s",
                    method,
                    url,
                    attempt_number,
                    cost_ms,
                    exc,
                )
                if attempt == attempts - 1:
                    raise AiDatalakeApiError(
                        f"HTTP {method} {url} failed: {exc}",
                        retryable=True,
                    ) from exc

            self._sleep_before_retry(attempt, retry_backoff)

        raise AiDatalakeApiError(f"HTTP {method} {url} failed: {last_error}", retryable=True)

    def _build_url(self, path_or_url: str) -> str:
        if path_or_url.startswith(("http://", "https://")):
            return path_or_url
        if not self.base_url:
            return path_or_url
        return f"{self.base_url}/{path_or_url.lstrip('/')}"

    @staticmethod
    def _should_retry_status(status_code: int) -> bool:
        return status_code == 429 or status_code >= 500

    @staticmethod
    def _sleep_before_retry(attempt: int, retry_backoff: tuple[float, ...]) -> None:
        if not retry_backoff:
            return
        time.sleep(retry_backoff[min(attempt, len(retry_backoff) - 1)])

    @staticmethod
    def _raise_api_error(response: requests.Response) -> None:
        message = response.text
        error_code = None
        request_id = None
        try:
            body = response.json()
            message = body.get("error_msg") or body.get("message") or response.text
            error_code = body.get("error_code")
            request_id = body.get("request_id")
        except ValueError:
            body = None

        raise AiDatalakeApiError(
            message or f"API request failed with status {response.status_code}",
            status_code=response.status_code,
            error_code=error_code,
            request_id=request_id,
            retryable=HttpClient._should_retry_status(response.status_code),
        )
