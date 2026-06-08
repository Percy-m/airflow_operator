"""Validation helpers for Ray operator parameters."""

from __future__ import annotations

from typing import Any

from custom_operator.ray.exceptions import AiDatalakeRayValidationError


def require_non_empty_string(name: str, value: str | None) -> None:
    if not isinstance(value, str) or not value.strip():
        raise AiDatalakeRayValidationError(f"{name} must be a non-empty string")


def validate_poll_interval(poll_interval: int) -> None:
    if poll_interval < 10:
        raise AiDatalakeRayValidationError("poll_interval must be at least 10 seconds")


def validate_non_negative_number(name: str, value: int | float | None) -> None:
    if value is None:
        return
    if not isinstance(value, (int, float)) or value < 0:
        raise AiDatalakeRayValidationError(f"{name} must be a non-negative number")


def validate_runtime_env(runtime_env: dict[str, Any] | None) -> None:
    if runtime_env is None:
        return
    if not isinstance(runtime_env, dict):
        raise AiDatalakeRayValidationError("runtime_env must be a dict")
    py_modules = runtime_env.get("py_modules")
    if py_modules is not None:
        if not isinstance(py_modules, list) or not all(isinstance(item, str) for item in py_modules):
            raise AiDatalakeRayValidationError("runtime_env.py_modules must be a list of strings")
    pip = runtime_env.get("pip")
    if pip is not None:
        if not isinstance(pip, list) or not all(isinstance(item, str) for item in pip):
            raise AiDatalakeRayValidationError("runtime_env.pip must be a list of strings")
    env_vars = runtime_env.get("env_vars")
    if env_vars is not None:
        if not isinstance(env_vars, dict) or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in env_vars.items()
        ):
            raise AiDatalakeRayValidationError("runtime_env.env_vars must be a map of strings")
