from custom_operator.ray.triggers.ray import RayJobTrigger


def test_ray_trigger_serializes_without_token_for_connection_mode():
    trigger = RayJobTrigger(
        ray_conn_id="ray",
        workspace_id="workspace-1",
        job_id="ray-job-1",
    )

    classpath, kwargs = trigger.serialize()

    assert classpath == "custom_operator.ray.triggers.ray.RayJobTrigger"
    assert kwargs["job_id"] == "ray-job-1"
    assert "token" not in kwargs


def test_ray_trigger_serializes_direct_test_token():
    trigger = RayJobTrigger(
        ray_conn_id=None,
        ray_base_url="https://ray-api.example.com",
        token="test-token",
        request_timeout=12,
        verify=False,
        workspace_id="workspace-1",
        job_id="ray-job-1",
    )

    _, kwargs = trigger.serialize()

    assert kwargs["ray_conn_id"] is None
    assert kwargs["ray_base_url"] == "https://ray-api.example.com"
    assert kwargs["token"] == "test-token"
    assert kwargs["request_timeout"] == 12
    assert kwargs["verify"] is False
