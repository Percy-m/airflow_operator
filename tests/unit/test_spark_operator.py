from custom_operator.spark.operators.spark import SparkOperator


class FakeTaskInstance:
    def __init__(self):
        self.values = {}
        self.records = []

    def xcom_push(self, *, key, value):
        self.values[key] = value
        self.records.append((key, value))


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


def test_spark_operator_sync_wait_refreshes_log_download_xcom_each_poll(monkeypatch):
    class SparkHookStub:
        def __init__(self):
            self.states = ["RUNNING", "SUCCEED"]
            self.detail_calls = 0

        def get_job_state(self, job_id):
            return {"job_id": job_id, "state": self.states.pop(0)}

        def get_job_detail(self, job_id):
            self.detail_calls += 1
            return {
                "job_id": job_id,
                "log_url": f"obs://bucket/logs/{job_id}-{self.detail_calls}.tar.gz",
            }

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
        poll_interval=0,
    )
    calls = []

    def create_log_download_result(*, job_id, log_url):
        calls.append((job_id, log_url))
        return {
            "spark_log_download_status": "available",
            "spark_log_download_url": f"https://obs.example.com/{job_id}-{len(calls)}.tar.gz",
        }

    monkeypatch.setattr(operator, "_create_log_download_result", create_log_download_result)
    ti = FakeTaskInstance()

    result = operator._sync_wait({"ti": ti}, SparkHookStub(), "job-1")

    assert result == "job-1"
    assert calls == [
        ("job-1", "obs://bucket/logs/job-1-1.tar.gz"),
        ("job-1", "obs://bucket/logs/job-1-2.tar.gz"),
    ]
    assert [
        value for key, value in ti.records if key == "spark_log_download_url"
    ] == [
        "https://obs.example.com/job-1-1.tar.gz",
        "https://obs.example.com/job-1-2.tar.gz",
    ]
    assert ti.values["log_url"] == "obs://bucket/logs/job-1-2.tar.gz"
    assert ti.values["spark_log_download_url"] == "https://obs.example.com/job-1-2.tar.gz"
