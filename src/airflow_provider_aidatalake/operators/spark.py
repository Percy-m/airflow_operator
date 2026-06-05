"""Airflow 3 SparkOperator for AiDatalake Spark jobs."""

from __future__ import annotations

import time
from typing import Any, Sequence
from uuid import uuid4

from airflow.exceptions import AirflowException
from airflow.sdk import BaseOperator

from airflow_provider_aidatalake.exceptions import AiDatalakeValidationError
from airflow_provider_aidatalake.hooks.spark import SparkHook
from airflow_provider_aidatalake.models.spark import FAILURE_STATES, SparkJobType, TERMINAL_STATES
from airflow_provider_aidatalake.triggers.spark import SparkJobTrigger
from airflow_provider_aidatalake.utils.obs_path import convert_obs_paths
from airflow_provider_aidatalake.utils.validation import (
    require_non_empty_string,
    validate_poll_interval,
    validate_type_specific_parameters,
)


class SparkOperator(BaseOperator):
    """Submit and monitor an AiDatalake Spark job.

    The default job type is ``spark_jar_job`` so example DAGs and minimal usage
    align with jar jobs unless explicitly configured otherwise.
    """

    template_fields: Sequence[str] = (
        "workspace_id",
        "name",
        "endpoint_name",
        "spark_version",
        "job_agency",
        "description",
        "catalog_name",
        "spark_jar_parameter",
        "spark_py_parameter",
        "spark_sql_scripting_parameter",
        "spark_base_url",
        "auth_url",
        "auth_body",
        "auth_headers",
        "resource_config",
        "spark_config",
        "image_config",
        "restore_strategy",
        "labels",
        "logging_config",
    )

    def __init__(
        self,
        *,
        spark_conn_id: str | None = None,
        auth_conn_id: str | None = None,
        spark_base_url: str | None = None,
        auth_url: str | None = None,
        auth_body: dict[str, Any] | None = None,
        auth_headers: dict[str, str] | None = None,
        request_timeout: int = 30,
        verify: bool = True,
        workspace_id: str,
        name: str,
        endpoint_name: str,
        spark_version: str,
        job_type: str = SparkJobType.JAR.value,
        spark_jar_parameter: dict[str, Any] | None = None,
        spark_py_parameter: dict[str, Any] | None = None,
        spark_sql_scripting_parameter: dict[str, Any] | None = None,
        job_agency: str | None = None,
        description: str | None = None,
        catalog_name: str | None = None,
        labels: list[dict[str, str]] | None = None,
        resource_config: dict[str, Any] | None = None,
        spark_config: dict[str, str] | None = None,
        image_config: dict[str, Any] | None = None,
        restore_strategy: dict[str, Any] | None = None,
        logging_config: dict[str, Any] | None = None,
        deferrable: bool = True,
        poll_interval: int = 30,
        max_poll_failures: int = 10,
        convert_obs_path: bool = True,
        local_obs_prefix: str = "/mnt/OBS",
        fetch_detail_on_poll: bool = True,
        enable_sql_scripting_job: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.spark_conn_id = spark_conn_id
        self.auth_conn_id = auth_conn_id
        self.spark_base_url = spark_base_url
        self.auth_url = auth_url
        self.auth_body = auth_body
        self.auth_headers = auth_headers
        self.request_timeout = request_timeout
        self.verify = verify
        self.workspace_id = workspace_id
        self.name = name
        self.endpoint_name = endpoint_name
        self.spark_version = spark_version
        self.job_type = job_type
        self.spark_jar_parameter = spark_jar_parameter
        self.spark_py_parameter = spark_py_parameter
        self.spark_sql_scripting_parameter = spark_sql_scripting_parameter
        self.job_agency = job_agency
        self.description = description
        self.catalog_name = catalog_name
        self.labels = labels
        self.resource_config = resource_config
        self.spark_config = spark_config
        self.image_config = image_config
        self.restore_strategy = restore_strategy
        self.logging_config = logging_config
        self.deferrable = deferrable
        self.poll_interval = poll_interval
        self.max_poll_failures = max_poll_failures
        self.convert_obs_path = convert_obs_path
        self.local_obs_prefix = local_obs_prefix
        self.fetch_detail_on_poll = fetch_detail_on_poll
        self.enable_sql_scripting_job = enable_sql_scripting_job
        self._job_id: str | None = None

    def execute(self, context: dict[str, Any]) -> str | None:
        dag_id = None
        dag = context.get("dag")
        if dag is not None:
            dag_id = getattr(dag, "dag_id", None)
        dag_run = context.get("dag_run")
        if dag_id is None and dag_run is not None:
            dag_id = getattr(dag_run, "dag_id", None)
        self.log.info(
            "SparkOperator execution started task_id=%s dag_id=%s job_type=%s deferrable=%s config_mode=%s",
            self.task_id,
            dag_id,
            self.job_type,
            self.deferrable,
            self._config_mode(),
        )
        self._validate()
        payload = self._build_payload()
        self.log.info(
            "Spark job payload built name=%s endpoint_name=%s spark_version=%s has_restore_strategy=%s",
            payload.get("name"),
            payload.get("endpoint_name"),
            payload.get("spark_version"),
            "restore_strategy" in payload,
        )
        hook = self._hook()
        client_token = str(uuid4())

        self.log.info(
            "Submitting Spark job name=%s job_type=%s workspace_id=%s endpoint_name=%s",
            self.name,
            self.job_type,
            self.workspace_id,
            self.endpoint_name,
        )
        job_id = hook.submit_job(payload, client_token=client_token)
        self._job_id = job_id
        self._xcom_push(context, "job_id", job_id)
        self._xcom_push(context, "spark_state", "PENDING")
        self.log.info("Spark job submitted successfully job_id=%s", job_id)

        if self.deferrable:
            self.log.info(
                "Deferring Spark job monitoring job_id=%s poll_interval=%s max_poll_failures=%s",
                job_id,
                self.poll_interval,
                self.max_poll_failures,
            )
            self.defer(
                trigger=SparkJobTrigger(
                    spark_conn_id=self.spark_conn_id,
                    auth_conn_id=self.auth_conn_id,
                    spark_base_url=self.spark_base_url,
                    auth_url=self.auth_url,
                    auth_body=self.auth_body,
                    auth_headers=self.auth_headers,
                    request_timeout=self.request_timeout,
                    verify=self.verify,
                    workspace_id=self.workspace_id,
                    job_id=job_id,
                    poll_interval=self.poll_interval,
                    max_poll_failures=self.max_poll_failures,
                    fetch_detail_on_poll=self.fetch_detail_on_poll,
                ),
                method_name="execute_complete",
            )
            return None

        return self._sync_wait(context, hook, job_id)

    def execute_complete(self, context: dict[str, Any], event: dict[str, Any] | None = None) -> str:
        if not event:
            raise AirflowException("Spark trigger returned empty event")

        job_id = event.get("job_id") or self._job_id
        state = event.get("state")
        message = event.get("message")
        log_url = event.get("log_url")

        if job_id:
            self._xcom_push(context, "job_id", job_id)
        if state:
            self._xcom_push(context, "spark_state", state)
        if log_url:
            self._xcom_push(context, "log_url", log_url)
        if message:
            self._xcom_push(context, "spark_job_message", message)

        if state == "SUCCEED":
            self.log.info("Spark job succeeded job_id=%s", job_id)
            return str(job_id)
        if state in FAILURE_STATES or event.get("status") == "failed":
            raise AirflowException(message or f"Spark job {job_id} failed with state {state}")
        raise AirflowException(f"Unexpected Spark trigger event: {event}")

    def on_kill(self) -> None:
        if not self._job_id:
            return
        try:
            self.log.info("Cancelling Spark job from on_kill job_id=%s", self._job_id)
            self._hook().cancel_job(self._job_id, check_state=True)
        except Exception as exc:
            self.log.exception("Failed to cancel Spark job from on_kill job_id=%s: %s", self._job_id, exc)

    def _sync_wait(self, context: dict[str, Any], hook: SparkHook, job_id: str) -> str:
        failure_count = 0
        while True:
            try:
                state_response = hook.get_job_state(job_id)
                state = state_response.get("state")
                self._xcom_push(context, "spark_state", state)
                if self.fetch_detail_on_poll:
                    try:
                        detail = hook.get_job_detail(job_id)
                        if detail.get("log_url"):
                            self._xcom_push(context, "log_url", detail["log_url"])
                    except Exception as exc:
                        self.log.warning("Failed to fetch Spark job detail job_id=%s: %s", job_id, exc)
                failure_count = 0
                if state in TERMINAL_STATES:
                    return self.execute_complete(
                        context,
                        {
                            "status": "success" if state == "SUCCEED" else "failed",
                            "job_id": job_id,
                            "state": state,
                        },
                    )
            except Exception as exc:
                failure_count += 1
                if failure_count >= self.max_poll_failures:
                    raise AirflowException(
                        f"Spark job polling failed {failure_count} consecutive times: {exc}"
                    ) from exc
                self.log.warning(
                    "Spark job polling failed job_id=%s failure_count=%s: %s",
                    job_id,
                    failure_count,
                    exc,
                )
            time.sleep(self.poll_interval)

    def _validate(self) -> None:
        require_non_empty_string("workspace_id", self.workspace_id)
        require_non_empty_string("name", self.name)
        require_non_empty_string("endpoint_name", self.endpoint_name)
        require_non_empty_string("spark_version", self.spark_version)
        validate_poll_interval(self.poll_interval)
        if self.max_poll_failures < 1:
            raise AiDatalakeValidationError("max_poll_failures must be greater than 0")
        validate_type_specific_parameters(
            job_type=self.job_type,
            spark_jar_parameter=self.spark_jar_parameter,
            spark_py_parameter=self.spark_py_parameter,
            spark_sql_scripting_parameter=self.spark_sql_scripting_parameter,
            enable_sql_scripting_job=self.enable_sql_scripting_job,
            local_obs_prefix=self.local_obs_prefix,
        )

    def _build_payload(self) -> dict[str, Any]:
        job_config = self._build_job_config()
        payload: dict[str, Any] = {
            "name": self.name,
            "endpoint_name": self.endpoint_name,
            "job_config": job_config,
            "spark_version": self.spark_version,
        }
        optional_fields = {
            "job_agency": self.job_agency,
            "resource_config": self.resource_config,
            "spark_config": self.spark_config,
            "image_config": self.image_config,
            "restore_strategy": self.restore_strategy,
            "description": self.description,
            "labels": self.labels,
            "catalog_name": self.catalog_name,
            "logging_config": self.logging_config,
        }
        payload.update({key: value for key, value in optional_fields.items() if value is not None})
        if self.convert_obs_path:
            payload = convert_obs_paths(payload, local_obs_prefix=self.local_obs_prefix)
        return payload

    def _build_job_config(self) -> dict[str, Any]:
        if self.job_type == SparkJobType.JAR.value:
            return {
                "job_type": self.job_type,
                "spark_jar_parameter": self.spark_jar_parameter or {},
            }
        if self.job_type == SparkJobType.PYTHON.value:
            return {
                "job_type": self.job_type,
                "spark_py_parameter": self.spark_py_parameter or {},
            }
        if self.job_type == SparkJobType.SQL_SCRIPTING.value:
            return {
                "job_type": self.job_type,
                "spark_sql_scripting_parameter": self.spark_sql_scripting_parameter or {},
            }
        raise AiDatalakeValidationError(f"Unsupported job_type: {self.job_type}")

    def _hook(self) -> SparkHook:
        return SparkHook(
            spark_conn_id=self.spark_conn_id,
            auth_conn_id=self.auth_conn_id,
            spark_base_url=self.spark_base_url,
            auth_url=self.auth_url,
            auth_body=self.auth_body,
            auth_headers=self.auth_headers,
            request_timeout=self.request_timeout,
            verify=self.verify,
            workspace_id=self.workspace_id,
        )

    def _config_mode(self) -> str:
        if self.spark_base_url and self.auth_url:
            return "direct"
        return "connection"

    @staticmethod
    def _xcom_push(context: dict[str, Any], key: str, value: Any) -> None:
        task_instance = context.get("ti") or context.get("task_instance")
        if task_instance is not None:
            task_instance.xcom_push(key=key, value=value)
