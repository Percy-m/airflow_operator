"""Validation helpers for Spark operator parameters."""

from __future__ import annotations

from typing import Any

from custom_operator.spark.exceptions import AiDatalakeValidationError
from custom_operator.spark.models.spark import SPARK_JOB_TYPES, SparkJobType
from custom_operator.spark.utils.obs_path import is_obs_or_convertible_path


def require_non_empty_string(name: str, value: str | None) -> None:
    if not isinstance(value, str) or not value.strip():
        raise AiDatalakeValidationError(f"{name} must be a non-empty string")


def validate_poll_interval(poll_interval: int) -> None:
    if poll_interval < 10:
        raise AiDatalakeValidationError("poll_interval must be at least 10 seconds")


def validate_job_type(job_type: str) -> None:
    if job_type not in SPARK_JOB_TYPES:
        raise AiDatalakeValidationError(
            f"job_type must be one of {sorted(SPARK_JOB_TYPES)}, got {job_type!r}"
        )


def validate_type_specific_parameters(
    *,
    job_type: str,
    spark_jar_parameter: dict[str, Any] | None,
    spark_py_parameter: dict[str, Any] | None,
    spark_sql_scripting_parameter: dict[str, Any] | None,
    enable_sql_scripting_job: bool,
    local_obs_prefix: str,
) -> None:
    validate_job_type(job_type)

    provided = {
        SparkJobType.JAR.value: spark_jar_parameter,
        SparkJobType.PYTHON.value: spark_py_parameter,
        SparkJobType.SQL_SCRIPTING.value: spark_sql_scripting_parameter,
    }

    for candidate_type, params in provided.items():
        if candidate_type != job_type and params:
            raise AiDatalakeValidationError(
                f"{candidate_type} parameters were provided but job_type is {job_type}"
            )

    params = provided[job_type]
    if not isinstance(params, dict) or not params:
        raise AiDatalakeValidationError(f"parameters for {job_type} must be provided")

    if job_type == SparkJobType.JAR.value:
        _validate_jar_parameters(params, local_obs_prefix=local_obs_prefix)
    elif job_type == SparkJobType.PYTHON.value:
        _validate_python_parameters(params, local_obs_prefix=local_obs_prefix)
    elif job_type == SparkJobType.SQL_SCRIPTING.value:
        if not enable_sql_scripting_job:
            raise AiDatalakeValidationError(
                "spark_sql_scripting_job is reserved and disabled by default; "
                "set enable_sql_scripting_job=True to submit it"
            )
        _validate_sql_scripting_parameters(params, local_obs_prefix=local_obs_prefix)


def _validate_jar_parameters(params: dict[str, Any], *, local_obs_prefix: str) -> None:
    main_jar = params.get("main_jar")
    require_non_empty_string("spark_jar_parameter.main_jar", main_jar)
    if not is_obs_or_convertible_path(main_jar, local_obs_prefix=local_obs_prefix):
        raise AiDatalakeValidationError("spark_jar_parameter.main_jar must be an OBS path")
    if not main_jar.lower().endswith(".jar"):
        raise AiDatalakeValidationError("spark_jar_parameter.main_jar must end with .jar")
    _validate_main_args(params.get("main_args"), "spark_jar_parameter.main_args")


def _validate_python_parameters(params: dict[str, Any], *, local_obs_prefix: str) -> None:
    main_python_file = params.get("main_python_file")
    require_non_empty_string("spark_py_parameter.main_python_file", main_python_file)
    if not is_obs_or_convertible_path(main_python_file, local_obs_prefix=local_obs_prefix):
        raise AiDatalakeValidationError("spark_py_parameter.main_python_file must be an OBS path")
    if not main_python_file.lower().endswith((".py", ".zip")):
        raise AiDatalakeValidationError(
            "spark_py_parameter.main_python_file must end with .py or .zip"
        )
    _validate_main_args(params.get("main_args"), "spark_py_parameter.main_args")


def _validate_sql_scripting_parameters(params: dict[str, Any], *, local_obs_prefix: str) -> None:
    sql_scripting_file = params.get("sql_scripting_file")
    require_non_empty_string(
        "spark_sql_scripting_parameter.sql_scripting_file", sql_scripting_file
    )
    if not is_obs_or_convertible_path(sql_scripting_file, local_obs_prefix=local_obs_prefix):
        raise AiDatalakeValidationError(
            "spark_sql_scripting_parameter.sql_scripting_file must be an OBS path"
        )


def _validate_main_args(value: Any, name: str) -> None:
    if value is None:
        return
    if not isinstance(value, list):
        raise AiDatalakeValidationError(f"{name} must be a list of strings")
    if len(value) > 100:
        raise AiDatalakeValidationError(f"{name} must contain at most 100 items")
    for item in value:
        if not isinstance(item, str):
            raise AiDatalakeValidationError(f"{name} must be a list of strings")
        if len(item) > 512:
            raise AiDatalakeValidationError(f"items in {name} must not exceed 512 characters")

