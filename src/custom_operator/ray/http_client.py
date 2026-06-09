"""Ray compatibility wrapper for the common HTTP client."""

from __future__ import annotations

import requests

from custom_operator.common.http_client import HttpClient as CommonHttpClient
from custom_operator.ray.exceptions import AiDatalakeRayApiError


class HttpClient(CommonHttpClient):
    """Synchronous HTTP client used by Ray operators and triggers."""

    def __init__(
        self,
        *,
        base_url: str = "",
        timeout: int = 30,
        verify: bool = True,
        session: requests.Session | None = None,
    ) -> None:
        super().__init__(
            base_url=base_url,
            timeout=timeout,
            verify=verify,
            session=session,
            api_error_cls=AiDatalakeRayApiError,
        )
