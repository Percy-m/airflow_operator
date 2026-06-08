"""Airflow Connection parsing for Spark operator runtime config."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from custom_operator.spark.exceptions import AiDatalakeAuthError


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
    """Resolve Spark endpoint and static token from DAG config or Airflow Connection."""

    if base_url and token:
        return ConnectionConfig(
            base_url=base_url,
            token=token,
            timeout=request_timeout,
            verify=verify,
        )

    conn = hook.get_connection(conn_id)
    extra = conn.extra_dejson or {}
    resolved_base_url = base_url or connection_base_url(conn)
    resolved_token = token or extra.get("token") or conn.password
    if not resolved_token:
        raise AiDatalakeAuthError(
            "Spark token must be configured in DAG token, "
            "Connection extra.token, or Connection password"
        )

    return ConnectionConfig(
        base_url=resolved_base_url,
        token=str(resolved_token),
        timeout=int(extra.get("timeout", request_timeout)),
        verify=as_bool(extra.get("verify", verify)),
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


def as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() not in {"0", "false", "no", "off"}
    return bool(value)
