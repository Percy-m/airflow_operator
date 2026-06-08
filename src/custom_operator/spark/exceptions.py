"""Provider specific exceptions."""

from __future__ import annotations


class AiDatalakeError(Exception):
    """Base exception for AiDatalake provider errors."""


class AiDatalakeAuthError(AiDatalakeError):
    """Raised when token acquisition or authentication fails."""


class AiDatalakeApiError(AiDatalakeError):
    """Raised when AiDatalake Spark API returns an error."""

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


class AiDatalakeValidationError(AiDatalakeError, ValueError):
    """Raised when operator parameters are invalid."""

