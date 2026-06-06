from unittest.mock import Mock

from airflow_provider_aidatalake_ray.clients.ray_api import RayApiClient


def test_create_job_uses_ray_api_and_auth_header():
    response = Mock()
    response.json.return_value = {"id": "ray-job-1"}
    http_client = Mock()
    http_client.request.return_value = response
    token_provider = Mock()
    token_provider.get_token.return_value = "token-1"

    client = RayApiClient(http_client=http_client, token_provider=token_provider)

    job_id = client.create_job(
        workspace_id="workspace-1",
        payload={"name": "demo"},
        transaction_id="transaction-1",
    )

    assert job_id == "ray-job-1"
    http_client.request.assert_called_once_with(
        "POST",
        "/v2/workspaces/workspace-1/ray-jobs",
        headers={
            "X-Auth-Token": "token-1",
            "Content-Type": "application/json",
            "X-Transaction-ID": "transaction-1",
        },
        json={"name": "demo"},
        expected_statuses={202},
        retry=3,
    )


def test_get_job_detail_uses_auth_header():
    response = Mock()
    response.json.return_value = {"id": "ray-job-1", "state": "RUNNING"}
    http_client = Mock()
    http_client.request.return_value = response
    token_provider = Mock()
    token_provider.get_token.return_value = "token-1"

    client = RayApiClient(http_client=http_client, token_provider=token_provider)

    assert client.get_job_detail(workspace_id="workspace-1", job_id="ray-job-1")["state"] == "RUNNING"
    http_client.request.assert_called_once_with(
        "GET",
        "/v2/workspaces/workspace-1/ray-jobs/ray-job-1",
        headers={"X-Auth-Token": "token-1", "Content-Type": "application/json"},
        expected_statuses={200},
        retry=0,
    )


def test_cancel_job_uses_auth_header():
    http_client = Mock()
    token_provider = Mock()
    token_provider.get_token.return_value = "token-1"
    client = RayApiClient(http_client=http_client, token_provider=token_provider)

    client.cancel_job(workspace_id="workspace-1", job_id="ray-job-1")

    http_client.request.assert_called_once_with(
        "POST",
        "/v2/workspaces/workspace-1/ray-jobs/ray-job-1/cancel",
        headers={"X-Auth-Token": "token-1", "Content-Type": "application/json"},
        expected_statuses={202},
        retry=2,
    )
