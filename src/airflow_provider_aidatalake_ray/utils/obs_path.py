"""OBS path conversion utilities for Ray structured runtime_env fields."""

from __future__ import annotations

from typing import Any


def convert_local_obs_path(value: str, *, local_obs_prefix: str = "/mnt/OBS") -> str:
    """Convert /mnt/OBS/bucket/path to obs://bucket/path."""

    normalized_prefix = local_obs_prefix.rstrip("/")
    if not value.startswith(normalized_prefix + "/"):
        return value
    suffix = value[len(normalized_prefix) + 1 :]
    if not suffix:
        return value
    return f"obs://{suffix}"


def convert_runtime_env_obs_paths(
    runtime_env: dict[str, Any] | None,
    *,
    local_obs_prefix: str = "/mnt/OBS",
) -> dict[str, Any] | None:
    """Convert only Ray runtime_env path fields, leaving entrypoint untouched."""

    if runtime_env is None:
        return None
    converted = dict(runtime_env)
    working_dir = converted.get("working_dir")
    if isinstance(working_dir, str):
        converted["working_dir"] = convert_local_obs_path(
            working_dir,
            local_obs_prefix=local_obs_prefix,
        )
    py_modules = converted.get("py_modules")
    if isinstance(py_modules, list):
        converted["py_modules"] = [
            convert_local_obs_path(item, local_obs_prefix=local_obs_prefix)
            if isinstance(item, str)
            else item
            for item in py_modules
        ]
    return converted
