"""Spark job constants and lightweight helpers."""

from __future__ import annotations

from enum import Enum
from typing import Any, TypedDict


class SparkJobType(str, Enum):
    JAR = "spark_jar_job"
    PYTHON = "spark_python_job"
    SQL_SCRIPTING = "spark_sql_scripting_job"


class SparkJobState(str, Enum):
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    CANCELING = "CANCELING"
    CANCELED = "CANCELED"
    FAILED = "FAILED"
    QUEUED_TIMEOUT = "QUEUED_TIMEOUT"
    RUNNING_TIMEOUT = "RUNNING_TIMEOUT"
    SUCCEED = "SUCCEED"


TERMINAL_STATES = {
    SparkJobState.SUCCEED.value,
    SparkJobState.FAILED.value,
    SparkJobState.CANCELED.value,
    SparkJobState.QUEUED_TIMEOUT.value,
    SparkJobState.RUNNING_TIMEOUT.value,
}

CANCELABLE_STATES = {
    SparkJobState.PENDING.value,
    SparkJobState.QUEUED.value,
    SparkJobState.RUNNING.value,
}

FAILURE_STATES = TERMINAL_STATES - {SparkJobState.SUCCEED.value}

SPARK_JOB_TYPES = {job_type.value for job_type in SparkJobType}


class SparkTriggerEvent(TypedDict, total=False):
    status: str
    job_id: str
    state: str
    message: str
    log_url: str | None
    spark_log_download_status: str
    spark_log_download_url: str
    spark_log_download_expires_at: str
    spark_log_file_size: int
    spark_log_file_exists: bool
    spark_log_download_message: str
    spark_log_download_error_code: str
    detail: dict[str, Any] | None
