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

