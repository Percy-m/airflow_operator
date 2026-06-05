from airflow_provider_aidatalake.hooks.spark import SparkHook


def test_spark_hook_direct_config_does_not_read_connection(monkeypatch):
    def fail_get_connection(*args, **kwargs):
        raise AssertionError("get_connection should not be called in direct config mode")

    monkeypatch.setattr(SparkHook, "get_connection", fail_get_connection)

    hook = SparkHook(
        workspace_id="workspace-1",
        spark_base_url="https://spark-api.example.com",
        auth_url="https://auth.example.com/token",
        auth_body={"auth": "body"},
        auth_headers={"X-Test": "1"},
        request_timeout=12,
        verify=False,
    )

    client = hook.client

    assert client.http_client.base_url == "https://spark-api.example.com"
    assert client.http_client.timeout == 12
    assert client.http_client.verify is False
    assert client.token_provider.auth_url == "https://auth.example.com/token"
    assert client.token_provider.auth_body == {"auth": "body"}
    assert client.token_provider.auth_headers == {"X-Test": "1"}
