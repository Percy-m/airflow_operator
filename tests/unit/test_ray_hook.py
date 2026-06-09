from types import SimpleNamespace

import pytest

from custom_operator.ray.exceptions import AiDatalakeRayAuthError
from custom_operator.ray.hooks.ray import RayHook


def test_ray_hook_direct_config_does_not_read_connection(monkeypatch):
    def fail_get_connection(*args, **kwargs):
        raise AssertionError("get_connection should not be called in direct token mode")

    monkeypatch.setattr(RayHook, "get_connection", fail_get_connection)

    hook = RayHook(
        workspace_id="workspace-1",
        ray_base_url="https://ray-api.example.com",
        token="test-token",
        request_timeout=12,
        verify=False,
    )

    client = hook.client

    assert client.http_client.base_url == "https://ray-api.example.com"
    assert client.http_client.timeout == 12
    assert client.http_client.verify is False
    assert client.token_provider.get_token() == "test-token"


def test_ray_hook_reads_token_from_connection_password(monkeypatch):
    conn = SimpleNamespace(
        host="ray-api.example.com",
        schema="https",
        port=None,
        password="connection-token",
        extra_dejson={},
    )
    monkeypatch.setattr(RayHook, "get_connection", lambda self, conn_id: conn)

    hook = RayHook(workspace_id="workspace-1", ray_conn_id="ray_conn")

    client = hook.client

    assert client.http_client.base_url == "https://ray-api.example.com"
    assert client.token_provider.get_token() == "connection-token"


def test_ray_hook_ignores_connection_extra(monkeypatch):
    conn = SimpleNamespace(
        host="https://ray-api.example.com",
        schema=None,
        port=None,
        password="connection-token",
        extra_dejson={"token": "extra-token", "timeout": 15, "verify": "false"},
    )
    monkeypatch.setattr(RayHook, "get_connection", lambda self, conn_id: conn)

    hook = RayHook(
        workspace_id="workspace-1",
        ray_conn_id="ray_conn",
        request_timeout=12,
        verify=True,
    )

    client = hook.client

    assert client.http_client.base_url == "https://ray-api.example.com"
    assert client.http_client.timeout == 12
    assert client.http_client.verify is True
    assert client.token_provider.get_token() == "connection-token"


def test_ray_hook_requires_token(monkeypatch):
    conn = SimpleNamespace(
        host="ray-api.example.com",
        schema="https",
        port=None,
        password=None,
        extra_dejson={"token": "extra-token"},
    )
    monkeypatch.setattr(RayHook, "get_connection", lambda self, conn_id: conn)

    hook = RayHook(workspace_id="workspace-1", ray_conn_id="ray_conn")

    with pytest.raises(AiDatalakeRayAuthError, match="Ray token must be configured"):
        _ = hook.client
