"""Static Ray X-Auth-Token helpers."""

from __future__ import annotations


def mask_token(token: str | None) -> str:
    """Return a log-safe token preview."""

    if not token:
        return "<empty>"
    if len(token) < 5:
        return "***"
    return f"{token[:5]}***"


class StaticTokenProvider:
    """Provides a fixed X-Auth-Token from DAG config or Airflow Connection."""

    def __init__(self, token: str) -> None:
        self._token = token

    def get_token(self) -> str:
        return self._token

    def refresh_token(self) -> str:
        return self._token
