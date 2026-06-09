"""Ray compatibility wrapper for common Connection parsing."""

from __future__ import annotations

from custom_operator.common.connection import (
    ConnectionConfig,
    ConnectionReader,
    connection_base_url,
    resolve_connection_config as _resolve_connection_config,
)
from custom_operator.ray.exceptions import AiDatalakeRayAuthError


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
    return _resolve_connection_config(
        hook,
        conn_id=conn_id,
        base_url=base_url,
        token=token,
        request_timeout=request_timeout,
        verify=verify,
        auth_error_cls=AiDatalakeRayAuthError,
        service_name="Ray",
    )
