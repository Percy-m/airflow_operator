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


def test_spark_trigger_serializes_log_download_config():
    trigger = SparkJobTrigger(
        spark_conn_id=None,
        spark_base_url="https://spark-api.example.com",
        token="test-token",
        workspace_core_base_url="https://workspace-core.example.com",
        workspace_core_internal_token="internal-token",
        log_download_timeout=5,
        workspace_id="workspace-1",
        job_id="job-1",
    )

    _, kwargs = trigger.serialize()

    assert kwargs["workspace_core_base_url"] == "https://workspace-core.example.com"
    assert kwargs["workspace_core_internal_token"] == "internal-token"
    assert kwargs["log_download_timeout"] == 5
    assert kwargs["enable_log_download"] is True


def test_spark_trigger_refreshes_log_download_url_each_poll(monkeypatch):
    class SparkHookStub:
        def get_job_state(self, job_id):
            return {"job_id": job_id, "state": "RUNNING"}

        def get_job_detail(self, job_id):
            return {"job_id": job_id, "log_url": f"obs://bucket/logs/{job_id}.tar.gz"}

    class LogDownloadHookStub:
        def __init__(self):
            self.calls = []

        def create_download_url(self, *, job_id, log_path):
            self.calls.append((job_id, log_path))
            call_number = len(self.calls)
            return {
                "spark_log_download_status": "available",
                "spark_log_download_url": (
                    f"https://obs.example.com/{job_id}-{call_number}.tar.gz"
                ),
            }

    log_hook = LogDownloadHookStub()
    trigger = SparkJobTrigger(
        spark_conn_id=None,
        workspace_core_base_url="https://workspace-core.example.com",
        workspace_core_internal_token="internal-token",
        workspace_id="workspace-1",
        job_id="job-1",
    )
    monkeypatch.setattr(trigger, "_hook", lambda: SparkHookStub())
    monkeypatch.setattr(trigger, "_log_download_hook", lambda: log_hook)

    first_event = trigger._poll_once()
    second_event = trigger._poll_once()

    assert log_hook.calls == [
        ("job-1", "obs://bucket/logs/job-1.tar.gz"),
        ("job-1", "obs://bucket/logs/job-1.tar.gz"),
    ]
    assert first_event["spark_log_download_status"] == "available"
    assert first_event["spark_log_download_url"] == "https://obs.example.com/job-1-1.tar.gz"
    assert second_event["spark_log_download_url"] == "https://obs.example.com/job-1-2.tar.gz"
