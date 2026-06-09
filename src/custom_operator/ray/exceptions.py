"""Ray provider specific exceptions."""

from __future__ import annotations

from custom_operator.common.exceptions import (
    AiDatalakeApiError as CommonAiDatalakeApiError,
    AiDatalakeAuthError as CommonAiDatalakeAuthError,
    AiDatalakeError as CommonAiDatalakeError,
    AiDatalakeValidationError as CommonAiDatalakeValidationError,
)


class AiDatalakeRayError(CommonAiDatalakeError):
    """Base exception for AiDatalake Ray provider errors."""


class AiDatalakeRayAuthError(CommonAiDatalakeAuthError, AiDatalakeRayError):
    """Raised when a Ray API token cannot be resolved."""


class AiDatalakeRayApiError(CommonAiDatalakeApiError, AiDatalakeRayError):
    """Raised when AiDatalake Ray API returns an error."""


class AiDatalakeRayValidationError(CommonAiDatalakeValidationError, AiDatalakeRayError):
    """Raised when Ray operator parameters are invalid."""
