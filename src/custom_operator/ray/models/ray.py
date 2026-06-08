"""Ray job constants and response helpers."""

from __future__ import annotations

from enum import Enum
from typing import Any, TypedDict


class RayJobState(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    STOPPED = "STOPPED"


RUNNING_STATES = {
    RayJobState.PENDING.value,
    RayJobState.RUNNING.value,
}

TERMINAL_STATES = {
    RayJobState.SUCCEEDED.value,
    RayJobState.FAILED.value,
    RayJobState.STOPPED.value,
}

FAILURE_STATES = TERMINAL_STATES - {RayJobState.SUCCEEDED.value}
CANCELABLE_STATES = RUNNING_STATES


class RayTriggerEvent(TypedDict, total=False):
    status: str
    job_id: str
    state: str
    message: str
    detail: dict[str, Any] | None


def extract_job_state(detail: dict[str, Any]) -> str | None:
    """Extract Ray job state from several gateway response shapes."""

    if isinstance(detail.get("data"), dict):
        data_state = detail["data"].get("state") or detail["data"].get("status")
        if data_state:
            return str(data_state)
    state = detail.get("state") or detail.get("status")
    if state and str(state).upper() != "OK":
        return str(state)
    return None


def extract_job_id(detail: dict[str, Any], default: str) -> str:
    """Extract Ray job id from several gateway response shapes."""

    if isinstance(detail.get("data"), dict):
        data_job_id = detail["data"].get("job_id") or detail["data"].get("id")
        if data_job_id:
            return str(data_job_id)
    return str(detail.get("job_id") or detail.get("id") or default)
