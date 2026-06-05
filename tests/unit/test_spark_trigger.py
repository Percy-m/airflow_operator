from airflow_provider_aidatalake.triggers.spark import SparkJobTrigger


def test_spark_trigger_serializes_without_token():
    trigger = SparkJobTrigger(
        spark_conn_id="spark",
        auth_conn_id="auth",
        workspace_id="workspace-1",
        job_id="job-1",
    )

    classpath, kwargs = trigger.serialize()

    assert classpath == "airflow_provider_aidatalake.triggers.spark.SparkJobTrigger"
    assert kwargs["job_id"] == "job-1"
    assert "token" not in kwargs


def test_spark_trigger_serializes_direct_config():
    trigger = SparkJobTrigger(
        spark_conn_id=None,
        auth_conn_id=None,
        spark_base_url="https://spark-api.example.com",
        auth_url="https://auth.example.com/token",
        auth_body={"auth": "body"},
        auth_headers={"X-Test": "1"},
        request_timeout=12,
        verify=False,
        workspace_id="workspace-1",
        job_id="job-1",
    )

    _, kwargs = trigger.serialize()

    assert kwargs["spark_conn_id"] is None
    assert kwargs["auth_conn_id"] is None
    assert kwargs["spark_base_url"] == "https://spark-api.example.com"
    assert kwargs["auth_url"] == "https://auth.example.com/token"
    assert kwargs["auth_body"] == {"auth": "body"}
    assert kwargs["auth_headers"] == {"X-Test": "1"}
    assert kwargs["request_timeout"] == 12
    assert kwargs["verify"] is False
