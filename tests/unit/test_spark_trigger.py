from custom_operator.spark.triggers.spark import SparkJobTrigger


def test_spark_trigger_serializes_without_token():
    trigger = SparkJobTrigger(
        spark_conn_id="spark",
        workspace_id="workspace-1",
        job_id="job-1",
    )

    classpath, kwargs = trigger.serialize()

    assert classpath == "custom_operator.spark.triggers.spark.SparkJobTrigger"
    assert kwargs["job_id"] == "job-1"
    assert "token" not in kwargs


def test_spark_trigger_serializes_direct_config():
    trigger = SparkJobTrigger(
        spark_conn_id=None,
        spark_base_url="https://spark-api.example.com",
        token="test-token",
        request_timeout=12,
        verify=False,
        workspace_id="workspace-1",
        job_id="job-1",
    )

    _, kwargs = trigger.serialize()

    assert kwargs["spark_conn_id"] is None
    assert kwargs["spark_base_url"] == "https://spark-api.example.com"
    assert kwargs["token"] == "test-token"
    assert kwargs["request_timeout"] == 12
    assert kwargs["verify"] is False
