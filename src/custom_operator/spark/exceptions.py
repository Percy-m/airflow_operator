"""Provider specific exceptions."""

from __future__ import annotations

from custom_operator.common.exceptions import (
    AiDatalakeApiError as CommonAiDatalakeApiError,
    AiDatalakeAuthError as CommonAiDatalakeAuthError,
    AiDatalakeError as CommonAiDatalakeError,
    AiDatalakeValidationError as CommonAiDatalakeValidationError,
)


class AiDatalakeError(CommonAiDatalakeError):
    """Base exception for AiDatalake provider errors."""


class AiDatalakeAuthError(CommonAiDatalakeAuthError, AiDatalakeError):
    """Raised when token acquisition or authentication fails."""


class AiDatalakeApiError(CommonAiDatalakeApiError, AiDatalakeError):
    """Raised when AiDatalake Spark API returns an error."""


class AiDatalakeValidationError(CommonAiDatalakeValidationError, AiDatalakeError):
    """Raised when operator parameters are invalid."""
