"""Ray provider specific exceptions."""

from __future__ import annotations


class AiDatalakeRayError(Exception):
    """Base exception for AiDatalake Ray provider errors."""


class AiDatalakeRayAuthError(AiDatalakeRayError):
    """Raised when a Ray API token cannot be resolved."""


class AiDatalakeRayApiError(AiDatalakeRayError):
    """Raised when AiDatalake Ray API returns an error."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        error_code: str | None = None,
        request_id: str | None = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code
        self.request_id = request_id
        self.retryable = retryable


class AiDatalakeRayValidationError(AiDatalakeRayError, ValueError):
    """Raised when Ray operator parameters are invalid."""
