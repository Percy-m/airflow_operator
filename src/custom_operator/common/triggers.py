"""Common trigger base classes for AiDatalake compute jobs."""

from __future__ import annotations

import asyncio
from typing import Any, ClassVar

from airflow.triggers.base import BaseTrigger, TriggerEvent


class BaseJobTrigger(BaseTrigger):
    """Poll a compute job until it reaches a terminal state."""

    service_name: ClassVar[str] = "Compute"
    terminal_states: ClassVar[set[str]] = set()
    success_state: ClassVar[str] = ""

    def __init__(
        self,
        *,
        workspace_id: str,
        job_id: str,
        poll_interval: int = 30,
        max_poll_failures: int = 10,
    ) -> None:
        super().__init__()
        self.workspace_id = workspace_id
        self.job_id = job_id
        self.poll_interval = poll_interval
        self.max_poll_failures = max_poll_failures

    async def run(self):
        self.log.info(
            "%s trigger started job_id=%s poll_interval=%s",
            self.service_name,
            self.job_id,
            self.poll_interval,
        )
        failure_count = 0
        while True:
            try:
                event = await asyncio.to_thread(self._poll_once)
                failure_count = 0
                state = event.get("state")
                self.log.info(
                    "%s trigger poll succeeded job_id=%s state=%s",
                    self.service_name,
                    self.job_id,
                    state,
                )
                if state in self.terminal_states:
                    event["status"] = "success" if state == self.success_state else "failed"
                    yield TriggerEvent(event)
                    return
            except Exception as exc:
                failure_count += 1
                self.log.warning(
                    "%s trigger poll failed job_id=%s failure_count=%s: %s",
                    self.service_name,
                    self.job_id,
                    failure_count,
                    exc,
                )
                if failure_count >= self.max_poll_failures:
                    await asyncio.to_thread(self._on_poll_failures)
                    yield TriggerEvent(
                        {
                            "status": "failed",
                            "job_id": self.job_id,
                            "message": (
                                f"{self.service_name} job polling failed "
                                f"{failure_count} consecutive times: {exc}"
                            ),
                        }
                    )
                    return

            await asyncio.sleep(self.poll_interval)

    async def cleanup(self) -> None:
        self.log.info("%s trigger cleanup started job_id=%s", self.service_name, self.job_id)
        try:
            await asyncio.to_thread(self._cancel_for_cleanup)
        except Exception as exc:
            self.log.exception(
                "%s trigger cleanup failed for job_id=%s: %s",
                self.service_name,
                self.job_id,
                exc,
            )

    def _poll_once(self) -> dict[str, Any]:
        raise NotImplementedError

    def _cancel_for_cleanup(self) -> None:
        raise NotImplementedError

    def _on_poll_failures(self) -> None:
        return None
