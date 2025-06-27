import asyncio
import logging
from typing import Any, Dict, Optional
from contextlib import suppress
from nomad_api import NomadApiClient
from pipeline_log_db import PipelineLogDB


class JobContext:
    """
    Holds two futures per job:
      - alloc_fut: set once the job hits 'pending' or 'running'
      - done_fut:  set on 'complete' or 'failed'
    Also tracks pipeline_id for database updates.
    """

    __slots__ = ("meta", "alloc_fut", "done_fut", "got_alloc", "pipeline_id")

    def __init__(self, meta: Dict[str, Any], pipeline_id: str):
        loop = asyncio.get_running_loop()
        self.meta = meta
        self.pipeline_id = pipeline_id
        self.alloc_fut = loop.create_future()
        self.done_fut = loop.create_future()
        self.got_alloc = False


class NomadJobMonitor:
    """
    Listens to Nomad AllocationUpdated events and drives the two‐phase futures.
    """

    def __init__(self, nomad_client: NomadApiClient, log_db: Optional[PipelineLogDB] = None):
        self._client = nomad_client
        self._log_db = log_db
        self._contexts: Dict[str, JobContext] = {}
        self._task: asyncio.Task | None = None
        self._status_updates = []  # Buffer for batch updates

    async def start(self):
        if self._task is None or self._task.done():
            logging.info("Starting NomadJobMonitor loop")
            self._task = asyncio.create_task(self._run(), name="NomadJobMonitor")

    async def stop(self):
        if self._task and not self._task.done():
            logging.info("Stopping NomadJobMonitor")
            self._task.cancel()
            with suppress(asyncio.CancelledError):
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

        # Extract pipeline_id from meta
        pipeline_id = meta.get("pipeline_id", "")
        ctx = JobContext(meta, pipeline_id)
        self._contexts[job_id] = ctx
        return ctx

    def _map_nomad_status(self, nomad_status: str) -> str:
        """Map Nomad allocation status to our status system."""
        status_map = {
            "pending": "allocated",
            "running": "running",
            "complete": "succeeded",
            "failed": "failed",
            "lost": "failed",
            "restart": "failed"
        }
        return status_map.get(nomad_status.lower(), nomad_status.lower())

    async def _flush_status_updates(self):
        """Batch update job statuses to database."""
        if self._log_db and self._status_updates:
            try:
                await self._log_db.batch_update_job_status(self._status_updates)
                self._status_updates = []
            except Exception as e:
                logging.error(f"Failed to update job statuses in database: {e}")

    async def _run(self):
        try:
            # Create a task to periodically flush status updates
            flush_task = asyncio.create_task(self._periodic_flush())
            
            async for event in self._client.events.stream():
                payload = event.get("Payload", {}) or {}
                alloc = payload.get("Allocation", {}) or {}
                jid = alloc.get("JobID")
                if jid not in self._contexts:
                    continue

                ctx = self._contexts[jid]
                status = alloc.get("ClientStatus", "").lower()

                # Update database with job status
                if self._log_db and ctx.pipeline_id:
                    mapped_status = self._map_nomad_status(status)
                    
                    # Get existing job record to preserve stage and write_path
                    existing_job = await self._log_db.get_job_status(jid)
                    if existing_job:
                        stage = existing_job["stage"]
                        write_paths = existing_job["write_paths"]
                        self._status_updates.append((jid, ctx.pipeline_id, mapped_status, stage, write_paths))
                    
                    # Flush if buffer is getting large
                    if len(self._status_updates) >= 50:
                        await self._flush_status_updates()

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
                    
                    # Update database with final status and actual output paths
                    if self._log_db and ctx.pipeline_id:
                        # Extract output paths from meta
                        write_paths = []
                        if "output_path" in ctx.meta:
                            write_paths.append(ctx.meta["output_path"])
                        if "metrics_path" in ctx.meta and ctx.meta["metrics_path"]:
                            write_paths.append(ctx.meta["metrics_path"])
                        
                        # Get existing job record to preserve stage
                        existing_job = await self._log_db.get_job_status(jid)
                        if existing_job:
                            stage = existing_job["stage"]
                            self._status_updates.append((jid, ctx.pipeline_id, "succeeded", stage, write_paths))
                    
                    self._contexts.pop(jid, None)

                elif status in ("failed", "lost", "restart"):
                    if not ctx.done_fut.done():
                        ctx.done_fut.set_exception(
                            RuntimeError(f"Job {jid} failed (status={status})")
                        )
                    
                    # Update database with final failed status (no write_paths since job failed)
                    if self._log_db and ctx.pipeline_id:
                        # Get existing job record to preserve stage
                        existing_job = await self._log_db.get_job_status(jid)
                        if existing_job:
                            stage = existing_job["stage"]
                            self._status_updates.append((jid, ctx.pipeline_id, "failed", stage, []))
                    
                    self._contexts.pop(jid, None)

        except asyncio.CancelledError:
            logging.info("NomadJobMonitor cancelled")
            # Flush any remaining updates before exiting
            await self._flush_status_updates()
        except Exception:
            logging.exception("NomadJobMonitor encountered an error")
        finally:
            if 'flush_task' in locals():
                flush_task.cancel()
                with suppress(asyncio.CancelledError):
                    await flush_task
            logging.info("NomadJobMonitor exiting")

    async def _periodic_flush(self):
        """Periodically flush status updates to database."""
        try:
            while True:
                await asyncio.sleep(5)  # Flush every 5 seconds
                await self._flush_status_updates()
        except asyncio.CancelledError:
            pass
