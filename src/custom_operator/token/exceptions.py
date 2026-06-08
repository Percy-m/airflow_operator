"""Shared exceptions for custom operator infrastructure."""

from __future__ import annotations


class CustomOperatorError(Exception):
    """Base exception for custom operator shared modules."""


class CustomOperatorAuthError(CustomOperatorError):
    """Raised when X-Auth-Token cannot be resolved."""


class CustomOperatorApiError(CustomOperatorError):
    """Raised when an external API call returns an error."""

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
