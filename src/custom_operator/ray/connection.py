"""Airflow Connection parsing for Ray operator runtime config."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from custom_operator.ray.exceptions import AiDatalakeRayAuthError


@dataclass(frozen=True)
class ConnectionConfig:
    base_url: str
    token: str
    timeout: int
    verify: bool


class ConnectionReader(Protocol):
    def get_connection(self, conn_id: str): ...


def resolve_connection_config(
    hook: ConnectionReader,
    *,
    conn_id: str,
    base_url: str | None,
    token: str | None,
    request_timeout: int,
    verify: bool,
) -> ConnectionConfig:
    """Resolve Ray endpoint and static token from DAG config or Airflow Connection."""

    if base_url and token:
        return ConnectionConfig(
            base_url=base_url,
            token=token,
            timeout=request_timeout,
            verify=verify,
        )

    conn = hook.get_connection(conn_id)
    resolved_base_url = base_url or connection_base_url(conn)
    resolved_token = token or conn.password
    if not resolved_token:
        raise AiDatalakeRayAuthError("Ray token must be configured in Connection password")

    return ConnectionConfig(
        base_url=resolved_base_url,
        token=str(resolved_token),
        timeout=request_timeout,
        verify=verify,
    )


def connection_base_url(conn) -> str:
    if conn.host and conn.host.startswith(("http://", "https://")):
        base_url = conn.host
    else:
        schema = conn.schema or "https"
        host = conn.host or ""
        base_url = f"{schema}://{host}"
    if conn.port:
        base_url = f"{base_url}:{conn.port}"
    return base_url.rstrip("/")
