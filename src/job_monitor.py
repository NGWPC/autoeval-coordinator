import asyncio
import logging
from typing import Any, Dict

from nomad_api import NomadApiClient


class JobContext:
    """
    Holds two futures per job:
      - alloc_fut: set once the job hits 'pending' or 'running'
      - done_fut:  set on 'complete' or 'failed'
    """

    __slots__ = ("meta", "alloc_fut", "done_fut", "got_alloc")

    def __init__(self, meta: Dict[str, Any]):
        loop = asyncio.get_running_loop()
        self.meta = meta
        self.alloc_fut = loop.create_future()
        self.done_fut = loop.create_future()
        self.got_alloc = False


class NomadJobMonitor:
    """
    Listens to Nomad AllocationUpdated events and drives the two‐phase futures.
    """

    def __init__(self, nomad_client: NomadApiClient):
        self._client = nomad_client
        self._contexts: Dict[str, JobContext] = {}
        self._task: asyncio.Task | None = None

    async def start(self):
        if self._task is None or self._task.done():
            logging.info("Starting NomadJobMonitor loop")
            self._task = asyncio.create_task(self._run(), name="NomadJobMonitor")

    async def stop(self):
        if self._task and not self._task.done():
            logging.info("Stopping NomadJobMonitor")
            self._task.cancel()
            with asyncio.suppress(asyncio.CancelledError):
                await self._task

        for jid, ctx in self._contexts.items():
            if not ctx.alloc_fut.done():
                ctx.alloc_fut.set_exception(RuntimeError("Monitor stopped"))
            if not ctx.done_fut.done():
                ctx.done_fut.set_exception(RuntimeError("Monitor stopped"))

        self._contexts.clear()

    async def track_job(self, job_id: str, meta: Dict[str, Any]) -> JobContext:
        """
        Register a DispatchedJobID. Returns a JobContext with:
          - alloc_fut: timeout for scheduling
          - done_fut:  waits indefinitely for completion
        """
        if job_id in self._contexts:
            return self._contexts[job_id]

        ctx = JobContext(meta)
        self._contexts[job_id] = ctx
        return ctx

    async def _run(self):
        try:
            async for event in self._client.events.stream():
                payload = event.get("Payload", {}) or {}
                alloc = payload.get("Allocation", {}) or {}
                jid = alloc.get("JobID")
                if jid not in self._contexts:
                    continue

                ctx = self._contexts[jid]
                status = alloc.get("ClientStatus", "").lower()

                # PHASE 1: first 'pending' or 'running'
                if not ctx.got_alloc and status in ("pending", "running"):
                    ctx.got_alloc = True
                    if not ctx.alloc_fut.done():
                        ctx.alloc_fut.set_result(alloc)

                # PHASE 2: terminal
                if status == "complete":
                    if not ctx.done_fut.done():
                        output = ctx.meta.get("output_path")
                        ctx.done_fut.set_result(output)
                    self._contexts.pop(jid, None)

                elif status in ("failed", "lost", "restart"):
                    if not ctx.done_fut.done():
                        ctx.done_fut.set_exception(
                            RuntimeError(f"Job {jid} failed (status={status})")
                        )
                    self._contexts.pop(jid, None)

        except asyncio.CancelledError:
            logging.info("NomadJobMonitor cancelled")
        except Exception:
            logging.exception("NomadJobMonitor encountered an error")
        finally:
            logging.info("NomadJobMonitor exiting")
