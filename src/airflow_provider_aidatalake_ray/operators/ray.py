"""Airflow 3 RayOperator for AiDatalake Ray jobs."""

from __future__ import annotations

import time
from typing import Any, Sequence
from uuid import uuid4

from airflow.exceptions import AirflowException
from airflow.sdk import BaseOperator

from airflow_provider_aidatalake_ray.exceptions import AiDatalakeRayValidationError
from airflow_provider_aidatalake_ray.hooks.ray import RayHook
from airflow_provider_aidatalake_ray.models.ray import FAILURE_STATES, TERMINAL_STATES
from airflow_provider_aidatalake_ray.triggers.ray import RayJobTrigger
from airflow_provider_aidatalake_ray.utils.obs_path import convert_runtime_env_obs_paths
from airflow_provider_aidatalake_ray.utils.validation import (
    require_non_empty_string,
    validate_non_negative_number,
    validate_poll_interval,
    validate_runtime_env,
)


class RayOperator(BaseOperator):
    """Submit and monitor an AiDatalake Ray job."""

    template_fields: Sequence[str] = (
        "workspace_id",
        "name",
        "endpoint_name",
        "description",
        "entrypoint",
        "runtime_env",
        "ray_base_url",
        "token",
        "labels",
    )

    def __init__(
        self,
        *,
        ray_conn_id: str | None = None,
        ray_base_url: str | None = None,
        token: str | None = None,
        request_timeout: int = 30,
        verify: bool = True,
        workspace_id: str,
        endpoint_name: str,
        entrypoint: str,
        name: str | None = None,
        description: str | None = None,
        runtime_env: dict[str, Any] | None = None,
        labels: list[dict[str, str]] | None = None,
        entrypoint_num_cpus: int | float | None = None,
        entrypoint_num_gpus: int | float | None = None,
        entrypoint_memory: int | float | None = None,
        deferrable: bool = True,
        poll_interval: int = 30,
        max_poll_failures: int = 10,
        convert_obs_path: bool = True,
        local_obs_prefix: str = "/mnt/OBS",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.ray_conn_id = ray_conn_id
        self.ray_base_url = ray_base_url
        self.token = token
        self.request_timeout = request_timeout
        self.verify = verify
        self.workspace_id = workspace_id
        self.endpoint_name = endpoint_name
        self.entrypoint = entrypoint
        self.name = name
        self.description = description
        self.runtime_env = runtime_env
        self.labels = labels
        self.entrypoint_num_cpus = entrypoint_num_cpus
        self.entrypoint_num_gpus = entrypoint_num_gpus
        self.entrypoint_memory = entrypoint_memory
        self.deferrable = deferrable
        self.poll_interval = poll_interval
        self.max_poll_failures = max_poll_failures
        self.convert_obs_path = convert_obs_path
        self.local_obs_prefix = local_obs_prefix
        self._job_id: str | None = None

    def execute(self, context: dict[str, Any]) -> str | None:
        self._validate()
        payload = self._build_payload(context)
        hook = self._hook()
        transaction_id = str(uuid4())

        self.log.info(
            "Submitting Ray job name=%s workspace_id=%s endpoint_name=%s deferrable=%s",
            payload["name"],
            self.workspace_id,
            self.endpoint_name,
            self.deferrable,
        )
        job_id = hook.submit_job(payload, transaction_id=transaction_id)
        self._job_id = job_id
        self._xcom_push(context, "job_id", job_id)
        self._xcom_push(context, "ray_job_id", job_id)
        self._xcom_push(context, "ray_state", "PENDING")
        self.log.info("Ray job submitted successfully job_id=%s", job_id)

        if self.deferrable:
            self.defer(
                trigger=RayJobTrigger(
                    ray_conn_id=self.ray_conn_id,
                    ray_base_url=self.ray_base_url,
                    token=self.token,
                    request_timeout=self.request_timeout,
                    verify=self.verify,
                    workspace_id=self.workspace_id,
                    job_id=job_id,
                    poll_interval=self.poll_interval,
                    max_poll_failures=self.max_poll_failures,
                ),
                method_name="execute_complete",
            )
            return None

        return self._sync_wait(context, hook, job_id)

    def execute_complete(self, context: dict[str, Any], event: dict[str, Any] | None = None) -> str:
        if not event:
            raise AirflowException("Ray trigger returned empty event")

        job_id = event.get("job_id") or self._job_id
        state = event.get("state")
        message = event.get("message")

        if job_id:
            self._xcom_push(context, "job_id", job_id)
            self._xcom_push(context, "ray_job_id", job_id)
        if state:
            self._xcom_push(context, "ray_state", state)
        if event.get("detail"):
            self._xcom_push(context, "ray_detail", event["detail"])
        if message:
            self._xcom_push(context, "ray_job_message", message)

        if state == "SUCCEEDED":
            self.log.info("Ray job succeeded job_id=%s", job_id)
            return str(job_id)
        if state in FAILURE_STATES or event.get("status") == "failed":
            raise AirflowException(message or f"Ray job {job_id} failed with state {state}")
        raise AirflowException(f"Unexpected Ray trigger event: {event}")

    def on_kill(self) -> None:
        if not self._job_id:
            return
        try:
            self.log.info("Cancelling Ray job from on_kill job_id=%s", self._job_id)
            self._hook().cancel_job(self._job_id, check_state=True)
        except Exception as exc:
            self.log.exception("Failed to cancel Ray job from on_kill job_id=%s: %s", self._job_id, exc)

    def _sync_wait(self, context: dict[str, Any], hook: RayHook, job_id: str) -> str:
        failure_count = 0
        while True:
            try:
                state_response = hook.get_job_state(job_id)
                state = state_response.get("state")
                self._xcom_push(context, "ray_state", state)
                failure_count = 0
                if state in TERMINAL_STATES:
                    return self.execute_complete(
                        context,
                        {
                            "status": "success" if state == "SUCCEEDED" else "failed",
                            "job_id": job_id,
                            "state": state,
                            "detail": state_response.get("detail"),
                        },
                    )
            except Exception as exc:
                failure_count += 1
                if failure_count >= self.max_poll_failures:
                    try:
                        hook.cancel_job(job_id, check_state=False)
                    except Exception as cancel_exc:
                        self.log.warning("Failed to cancel Ray job after polling failures: %s", cancel_exc)
                    raise AirflowException(
                        f"Ray job polling failed {failure_count} consecutive times: {exc}"
                    ) from exc
                self.log.warning(
                    "Ray job polling failed job_id=%s failure_count=%s: %s",
                    job_id,
                    failure_count,
                    exc,
                )
            time.sleep(self.poll_interval)

    def _validate(self) -> None:
        require_non_empty_string("workspace_id", self.workspace_id)
        require_non_empty_string("endpoint_name", self.endpoint_name)
        require_non_empty_string("entrypoint", self.entrypoint)
        validate_runtime_env(self.runtime_env)
        validate_non_negative_number("entrypoint_num_cpus", self.entrypoint_num_cpus)
        validate_non_negative_number("entrypoint_num_gpus", self.entrypoint_num_gpus)
        validate_non_negative_number("entrypoint_memory", self.entrypoint_memory)
        validate_poll_interval(self.poll_interval)
        if self.max_poll_failures < 1:
            raise AiDatalakeRayValidationError("max_poll_failures must be greater than 0")

    def _build_payload(self, context: dict[str, Any] | None = None) -> dict[str, Any]:
        runtime_env = self.runtime_env
        if self.convert_obs_path:
            runtime_env = convert_runtime_env_obs_paths(
                runtime_env,
                local_obs_prefix=self.local_obs_prefix,
            )

        config: dict[str, Any] = {"entrypoint": self.entrypoint}
        if runtime_env:
            config["runtime_env"] = runtime_env

        optional_config = {
            "entrypoint_num_cpus": self.entrypoint_num_cpus,
            "entrypoint_num_gpus": self.entrypoint_num_gpus,
            "entrypoint_memory": self.entrypoint_memory,
        }
        config.update({key: value for key, value in optional_config.items() if value is not None})

        payload: dict[str, Any] = {
            "name": self.name or self._default_name(context or {}),
            "endpoint_name": self.endpoint_name,
            "config": config,
        }
        optional_fields = {
            "description": self.description,
            "labels": self.labels,
        }
        payload.update({key: value for key, value in optional_fields.items() if value is not None})
        return payload

    def _hook(self) -> RayHook:
        return RayHook(
            ray_conn_id=self.ray_conn_id,
            ray_base_url=self.ray_base_url,
            token=self.token,
            request_timeout=self.request_timeout,
            verify=self.verify,
            workspace_id=self.workspace_id,
        )

    def _default_name(self, context: dict[str, Any]) -> str:
        dag_id = None
        run_id = None
        dag = context.get("dag")
        if dag is not None:
            dag_id = getattr(dag, "dag_id", None)
        dag_run = context.get("dag_run")
        if dag_run is not None:
            dag_id = dag_id or getattr(dag_run, "dag_id", None)
            run_id = getattr(dag_run, "run_id", None)
        parts = [part for part in (dag_id, self.task_id, run_id) if part]
        return "-".join(parts)[:64] or self.task_id

    @staticmethod
    def _xcom_push(context: dict[str, Any], key: str, value: Any) -> None:
        task_instance = context.get("ti") or context.get("task_instance")
        if task_instance is not None:
            task_instance.xcom_push(key=key, value=value)
