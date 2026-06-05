from unittest.mock import Mock

from airflow_provider_aidatalake.clients.spark_api import SparkApiClient


def test_create_job_uses_spark_api_and_auth_header():
    response = Mock()
    response.json.return_value = {"job_id": "job-1"}
    http_client = Mock()
    http_client.request.return_value = response
    token_provider = Mock()
    token_provider.get_token.return_value = "token-1"

    client = SparkApiClient(http_client=http_client, token_provider=token_provider)

    job_id = client.create_job(
        workspace_id="workspace-1",
        payload={"name": "demo"},
        client_token="client-token",
    )

    assert job_id == "job-1"
    http_client.request.assert_called_once_with(
        "POST",
        "/v2/workspaces/workspace-1/spark-jobs",
        headers={
            "X-Auth-Token": "token-1",
            "Content-Type": "application/json",
            "X-Client-Token": "client-token",
        },
        json={"name": "demo"},
        expected_statuses={201},
        retry=0,
    )


def test_create_job_logs_request_path_headers_and_body(caplog):
    response = Mock()
    response.json.return_value = {"job_id": "job-1"}
    http_client = Mock()
    http_client.request.return_value = response
    token_provider = Mock()
    token_provider.get_token.return_value = "abcdef-token"
    client = SparkApiClient(http_client=http_client, token_provider=token_provider)

    with caplog.at_level("INFO", logger="airflow_provider_aidatalake.clients.spark_api"):
        client.create_job(
            workspace_id="workspace-1",
            payload={"name": "demo"},
            client_token="client-token",
        )

    log_output = "\n".join(record.getMessage() for record in caplog.records)
    assert "/v2/workspaces/workspace-1/spark-jobs" in log_output
    assert "'name': 'demo'" in log_output
    assert "abcde***" in log_output
    assert "clien***" in log_output
    assert "abcdef-token" not in log_output
    assert "client-token" not in log_output


def test_get_job_state_uses_state_endpoint():
    response = Mock()
    response.json.return_value = {"job_id": "job-1", "state": "RUNNING"}
    http_client = Mock()
    http_client.request.return_value = response
    token_provider = Mock()
    token_provider.get_token.return_value = "token-1"

    client = SparkApiClient(http_client=http_client, token_provider=token_provider)

    assert client.get_job_state(workspace_id="workspace-1", job_id="job-1")["state"] == "RUNNING"
    http_client.request.assert_called_once_with(
        "GET",
        "/v2/workspaces/workspace-1/spark-jobs/job-1/state",
        headers={"X-Auth-Token": "token-1", "Content-Type": "application/json"},
        expected_statuses={200},
        retry=0,
    )


def test_cancel_job_uses_cancel_endpoint():
    http_client = Mock()
    token_provider = Mock()
    token_provider.get_token.return_value = "token-1"
    client = SparkApiClient(http_client=http_client, token_provider=token_provider)

    client.cancel_job(workspace_id="workspace-1", job_id="job-1")

    http_client.request.assert_called_once_with(
        "POST",
        "/v2/workspaces/workspace-1/spark-jobs/job-1/cancel",
        headers={"X-Auth-Token": "token-1", "Content-Type": "application/json"},
        expected_statuses={204},
        retry=0,
    )
