"""Common operator base classes for AiDatalake compute jobs."""

from __future__ import annotations

from typing import Any, Callable

from airflow.sdk import BaseOperator


class BaseComputeOperator(BaseOperator):
    """Common helpers shared by compute operators."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._job_id: str | None = None

    @staticmethod
    def _xcom_push(context: dict[str, Any], key: str, value: Any) -> None:
        task_instance = context.get("ti") or context.get("task_instance")
        if task_instance is not None:
            task_instance.xcom_push(key=key, value=value)

    def _cancel_job_on_kill(self, *, service_name: str, hook_factory: Callable[[], Any]) -> None:
        if not self._job_id:
            return
        try:
            self.log.info("Cancelling %s job from on_kill job_id=%s", service_name, self._job_id)
            hook_factory().cancel_job(self._job_id, check_state=True)
        except Exception as exc:
            self.log.exception(
                "Failed to cancel %s job from on_kill job_id=%s: %s",
                service_name,
                self._job_id,
                exc,
            )
