"""OBS path conversion utilities."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def convert_local_obs_path(value: str, *, local_obs_prefix: str = "/mnt/OBS") -> str:
    """Convert a mounted OBS path to an OBS URI.

    Example: /mnt/OBS/bucket/path -> obs://bucket/path
    """

    normalized_prefix = local_obs_prefix.rstrip("/")
    if not value.startswith(normalized_prefix + "/"):
        return value

    suffix = value[len(normalized_prefix) + 1 :]
    if not suffix:
        return value
    return f"obs://{suffix}"


def convert_obs_paths(value: Any, *, local_obs_prefix: str = "/mnt/OBS") -> Any:
    """Recursively convert string values containing mounted OBS paths."""

    if isinstance(value, str):
        return convert_local_obs_path(value, local_obs_prefix=local_obs_prefix)
    if isinstance(value, Mapping):
        return {
            key: convert_obs_paths(item, local_obs_prefix=local_obs_prefix)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [convert_obs_paths(item, local_obs_prefix=local_obs_prefix) for item in value]
    if isinstance(value, tuple):
        return tuple(convert_obs_paths(item, local_obs_prefix=local_obs_prefix) for item in value)
    return value


def is_obs_or_convertible_path(value: str, *, local_obs_prefix: str = "/mnt/OBS") -> bool:
    """Return true when a value is an OBS URI or a mounted OBS path."""

    return value.startswith("obs://") or value.startswith(local_obs_prefix.rstrip("/") + "/")

