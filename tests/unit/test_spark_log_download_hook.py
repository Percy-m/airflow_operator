from types import SimpleNamespace

import pytest

from custom_operator.spark.exceptions import AiDatalakeAuthError
from custom_operator.spark.hooks.log_download import SparkLogDownloadHook


def test_log_download_hook_direct_config_does_not_read_connection(monkeypatch):
    def fail_get_connection(*args, **kwargs):
        raise AssertionError("get_connection should not be called in direct config mode")

    monkeypatch.setattr(SparkLogDownloadHook, "get_connection", fail_get_connection)

    hook = SparkLogDownloadHook(
        workspace_core_base_url="https://workspace-core.example.com",
        workspace_core_internal_token="internal-token",
        request_timeout=5,
        verify=False,
    )

    client = hook.client

    assert client.http_client.base_url == "https://workspace-core.example.com"
    assert client.http_client.timeout == 5
    assert client.http_client.verify is False
    assert client.internal_auth_token == "internal-token"


def test_log_download_hook_reads_connection_host_password(monkeypatch):
    conn = SimpleNamespace(
        host="workspace-core.example.com",
        schema="https",
        port=8443,
        password="password-token",
    )
    monkeypatch.setattr(SparkLogDownloadHook, "get_connection", lambda self, conn_id: conn)

    hook = SparkLogDownloadHook(workspace_core_conn_id="workspace_core")

    client = hook.client

    assert client.http_client.base_url == "https://workspace-core.example.com:8443"
    assert client.internal_auth_token == "password-token"


def test_log_download_hook_requires_config():
    hook = SparkLogDownloadHook()

    with pytest.raises(AiDatalakeAuthError, match="requires explicit base URL/token"):
        _ = hook.client
