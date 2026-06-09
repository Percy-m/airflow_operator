from types import SimpleNamespace

import pytest

from custom_operator.spark.exceptions import AiDatalakeAuthError
from custom_operator.spark.hooks.spark import SparkHook


def test_spark_hook_direct_config_does_not_read_connection(monkeypatch):
    def fail_get_connection(*args, **kwargs):
        raise AssertionError("get_connection should not be called in direct config mode")

    monkeypatch.setattr(SparkHook, "get_connection", fail_get_connection)

    hook = SparkHook(
        workspace_id="workspace-1",
        spark_base_url="https://spark-api.example.com",
        token="test-token",
        request_timeout=12,
        verify=False,
    )

    client = hook.client

    assert client.http_client.base_url == "https://spark-api.example.com"
    assert client.http_client.timeout == 12
    assert client.http_client.verify is False
    assert client.token_provider.get_token() == "test-token"


def test_spark_hook_reads_connection_host_password_and_ignores_extra(monkeypatch):
    conn = SimpleNamespace(
        host="https://spark-api.example.com",
        schema=None,
        port=None,
        password="password-token",
        extra_dejson={"token": "extra-token", "timeout": 15, "verify": "false"},
    )
    monkeypatch.setattr(SparkHook, "get_connection", lambda self, conn_id: conn)

    hook = SparkHook(
        workspace_id="workspace-1",
        spark_conn_id="spark_conn",
        request_timeout=12,
        verify=True,
    )

    client = hook.client

    assert client.http_client.base_url == "https://spark-api.example.com"
    assert client.http_client.timeout == 12
    assert client.http_client.verify is True
    assert client.token_provider.get_token() == "password-token"


def test_spark_hook_reads_token_from_connection_password(monkeypatch):
    conn = SimpleNamespace(
        host="spark-api.example.com",
        schema="https",
        port=None,
        password="password-token",
        extra_dejson={},
    )
    monkeypatch.setattr(SparkHook, "get_connection", lambda self, conn_id: conn)

    hook = SparkHook(workspace_id="workspace-1", spark_conn_id="spark_conn")

    client = hook.client

    assert client.http_client.base_url == "https://spark-api.example.com"
    assert client.token_provider.get_token() == "password-token"


def test_spark_hook_requires_token(monkeypatch):
    conn = SimpleNamespace(
        host="spark-api.example.com",
        schema="https",
        port=None,
        password=None,
        extra_dejson={"token": "extra-token"},
    )
    monkeypatch.setattr(SparkHook, "get_connection", lambda self, conn_id: conn)

    hook = SparkHook(workspace_id="workspace-1", spark_conn_id="spark_conn")

    with pytest.raises(AiDatalakeAuthError, match="Spark token must be configured"):
        _ = hook.client
