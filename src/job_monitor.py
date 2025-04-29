import asyncio
import logging
from contextlib import suppress
from typing import Any, Dict, Tuple

from nomad_api import NomadApiClient


class NomadJobMonitor:
    """
    Listens to Nomad AllocationUpdated events and completes or fails
    Futures for tracked DispatchedJobIDs.
    """

    def __init__(self, nomad_client: NomadApiClient):
        self._client = nomad_client
        # map job_id → (Future[str], dispatch_meta)
        self._tracked: Dict[str, Tuple[asyncio.Future, Dict[str, Any]]] = {}
        self._task: asyncio.Task | None = None

    async def start(self):
        if self._task is None or self._task.done():
            logging.info("Starting Nomad job monitor task...")
            self._task = asyncio.create_task(self._run(), name="NomadJobMonitorTask")

    async def stop(self):
        if self._task and not self._task.done():
            logging.info("Stopping Nomad job monitor...")
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

        # fail any still-pending futures
        for job_id, (fut, _) in list(self._tracked.items()):
            if not fut.done():
                fut.set_exception(RuntimeError("NomadJobMonitor stopped"))
        self._tracked.clear()

    async def track_job(
        self, job_id: str, dispatch_meta: Dict[str, Any]
    ) -> asyncio.Future:
        """
        Register a DispatchedJobID for monitoring.
        Returns a Future[str] that will be set to dispatch_meta["output_path"]
        or raised on failure.
        """
        if job_id in self._tracked:
            # already tracking
            return self._tracked[job_id][0]

        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._tracked[job_id] = (fut, dispatch_meta)
        return fut

    async def _run(self):
        """
        Pull events from the SSE stream and resolve tracked Futures:
          - ClientStatus='complete' → success (meta['output_path'])
          - else → failure
        """
        try:
            async for event in self._client.events.stream():
                payload = event.get("Payload", {}) or {}
                alloc = payload.get("Allocation", {}) or {}
                jid = alloc.get("JobID")
                if jid not in self._tracked:
                    continue

                fut, meta = self._tracked.pop(jid)
                status = alloc.get("ClientStatus", "").lower()

                if status == "complete":
                    out = meta.get("output_path")
                    if isinstance(out, str) and out.startswith("s3://"):
                        fut.set_result(out)
                        logging.info("Job %s completed → %s", jid, out)
                    else:
                        fut.set_exception(
                            RuntimeError(
                                f"Job {jid} completed but no valid output_path in meta"
                            )
                        )
                else:
                    fut.set_exception(
                        RuntimeError(f"Job {jid} failed (ClientStatus={status})")
                    )
                    logging.error("Job %s failed (ClientStatus=%s)", jid, status)

        except asyncio.CancelledError:
            logging.info("Nomad job monitor cancelled")
        except Exception:
            logging.exception("Unexpected error in Nomad job monitor")
        finally:
            logging.info("Nomad job monitor exiting")
