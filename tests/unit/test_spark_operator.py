from custom_operator.spark.operators.spark import SparkOperator


class FakeTaskInstance:
    def __init__(self):
        self.values = {}

    def xcom_push(self, *, key, value):
        self.values[key] = value


def test_spark_operator_defaults_to_jar_job_and_builds_payload():
    operator = SparkOperator(
        task_id="spark_jar",
        workspace_id="workspace-1",
        name="demo",
        endpoint_name="endpoint1",
        spark_version="3.3.2",
        spark_jar_parameter={
            "main_class": "com.example.Main",
            "main_jar": "/mnt/OBS/bucket/main.jar",
            "main_args": ["--input", "/mnt/OBS/bucket/input"],
        },
    )

    operator._validate()
    payload = operator._build_payload()

    assert payload["job_config"]["job_type"] == "spark_jar_job"
    assert payload["job_config"]["spark_jar_parameter"]["main_jar"] == "obs://bucket/main.jar"
    assert payload["job_config"]["spark_jar_parameter"]["main_args"] == [
        "--input",
        "obs://bucket/input",
    ]


def test_spark_operator_accepts_direct_config():
    operator = SparkOperator(
        task_id="spark_jar",
        spark_base_url="https://spark-api.example.com",
        token="test-token",
        request_timeout=12,
        verify=False,
        workspace_id="workspace-1",
        name="demo",
        endpoint_name="endpoint1",
        spark_version="3.3.2",
        spark_jar_parameter={
            "main_class": "com.example.Main",
            "main_jar": "obs://bucket/main.jar",
        },
    )

    hook = operator._hook()

    assert hook.spark_base_url == "https://spark-api.example.com"
    assert hook.token == "test-token"
    assert hook.request_timeout == 12
    assert hook.verify is False


def test_spark_operator_execute_complete_pushes_log_download_xcom():
    operator = SparkOperator(
        task_id="spark_jar",
        workspace_id="workspace-1",
        name="demo",
        endpoint_name="endpoint1",
        spark_version="3.3.2",
        spark_jar_parameter={
            "main_class": "com.example.Main",
            "main_jar": "obs://bucket/main.jar",
        },
    )
    ti = FakeTaskInstance()

    result = operator.execute_complete(
        {"ti": ti},
        {
            "status": "success",
            "job_id": "job-1",
            "state": "SUCCEED",
            "log_url": "obs://bucket/logs/job-1.tar.gz",
            "spark_log_download_status": "available",
            "spark_log_download_url": "https://obs.example.com/job-1.tar.gz",
            "spark_log_download_expires_at": "2026-05-30T10:00:00Z",
            "spark_log_file_size": 1024,
            "spark_log_file_exists": True,
        },
    )

    assert result == "job-1"
    assert ti.values["job_id"] == "job-1"
    assert ti.values["spark_state"] == "SUCCEED"
    assert ti.values["log_url"] == "obs://bucket/logs/job-1.tar.gz"
    assert ti.values["spark_log_download_status"] == "available"
    assert ti.values["spark_log_download_url"] == "https://obs.example.com/job-1.tar.gz"
    assert ti.values["spark_log_file_size"] == 1024
