from unittest.mock import Mock

from custom_operator.spark.clients.log_download import SparkLogDownloadClient


def test_create_download_url_uses_workspace_core_api_and_internal_token():
    response = Mock()
    response.json.return_value = {
        "status": "OK",
        "errorCode": None,
        "errorMsg": None,
        "data": {
            "downloadUrl": "https://obs.example.com/logs.tar.gz?signature=1",
            "expiresAt": "2026-05-30T10:00:00Z",
            "fileSize": 1024,
            "fileExists": True,
        },
    }
    http_client = Mock()
    http_client.request.return_value = response

    client = SparkLogDownloadClient(
        http_client=http_client,
        internal_auth_token="internal-token",
    )

    result = client.create_download_url(
        job_id="job-1",
        log_path="obs://bucket/logs/job-1/spark-logs.tar.gz",
    )

    assert result == {
        "spark_log_download_status": "available",
        "spark_log_download_url": "https://obs.example.com/logs.tar.gz?signature=1",
        "spark_log_download_expires_at": "2026-05-30T10:00:00Z",
        "spark_log_file_size": 1024,
        "spark_log_file_exists": True,
    }
    http_client.request.assert_called_once_with(
        "POST",
        "/internal/log/v1/create",
        headers={
            "X-Internal-Auth-Token": "internal-token",
            "Content-Type": "application/json",
        },
        json={
            "jobId": "job-1",
            "logPath": "obs://bucket/logs/job-1/spark-logs.tar.gz",
        },
        expected_statuses={200},
        retry=0,
    )


def test_create_download_url_maps_file_missing_to_pending():
    response = Mock()
    response.json.return_value = {
        "status": "ERROR",
        "errorCode": "AIPDWS.BIZ.2050001",
        "errorMsg": "log file not found",
        "data": None,
    }
    http_client = Mock()
    http_client.request.return_value = response
    client = SparkLogDownloadClient(http_client=http_client, internal_auth_token="token")

    result = client.create_download_url(job_id="job-1", log_path="obs://bucket/logs/job-1")

    assert result == {
        "spark_log_download_status": "pending",
        "spark_log_download_message": "log file not found",
        "spark_log_download_error_code": "AIPDWS.BIZ.2050001",
    }


def test_create_download_url_maps_file_too_large():
    response = Mock()
    response.json.return_value = {
        "status": "ERROR",
        "errorCode": "AIPDWS.BIZ.2050002",
        "errorMsg": "log file too large",
        "data": None,
    }
    http_client = Mock()
    http_client.request.return_value = response
    client = SparkLogDownloadClient(http_client=http_client, internal_auth_token="token")

    result = client.create_download_url(job_id="job-1", log_path="obs://bucket/logs/job-1")

    assert result["spark_log_download_status"] == "too_large"
    assert result["spark_log_download_error_code"] == "AIPDWS.BIZ.2050002"
