import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

import aiohttp
import nomad
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


class JobStatus(Enum):
    DISPATCHED = "dispatched"
    ALLOCATED = "allocated"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    LOST = "lost"
    UNKNOWN = "unknown"


# Mapping from Nomad statuses to our internal statuses
NOMAD_STATUS_MAP = {
    "pending": JobStatus.ALLOCATED,
    "running": JobStatus.RUNNING,
    "complete": JobStatus.SUCCEEDED,
    "failed": JobStatus.FAILED,
    "lost": JobStatus.LOST,
    "dead": JobStatus.FAILED,
}


class NomadError(Exception):
    pass


class JobNotFoundError(NomadError):
    pass


class JobDispatchError(NomadError):
    pass


@dataclass
class JobTracker:
    """Tracks the state of a single Nomad job."""

    job_id: str
    pipeline_id: str
    dispatch_time: float = field(default_factory=time.time)
    status: JobStatus = JobStatus.DISPATCHED
    allocation_id: Optional[str] = None
    exit_code: Optional[int] = None
    completion_event: asyncio.Event = field(default_factory=asyncio.Event)
    error: Optional[Exception] = None
    task_name: Optional[str] = None
    stage: Optional[str] = None
    timestamp: Optional[datetime] = None


class NomadJobManager:
    def __init__(
        self,
        nomad_addr: str,
        namespace: str = "default",
        token: Optional[str] = None,
        session: Optional[aiohttp.ClientSession] = None,
        log_db: Optional[Any] = None,
    ):
        self.nomad_addr = nomad_addr
        self.namespace = namespace
        self.session = session
        self.log_db = log_db

        parsed = urlparse(str(nomad_addr))
        self.client = nomad.Nomad(
            host=parsed.hostname,
            port=parsed.port,
            verify=False,
            token=token or None,
            namespace=namespace or None,
        )

        # Track active jobs
        self._active_jobs: Dict[str, JobTracker] = {}
        self._monitoring_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

        self._event_index = 0

    async def start(self):
        if not self._monitoring_task:
            self._monitoring_task = asyncio.create_task(self._monitor_events())
            logger.info("Started Nomad job manager")

    async def stop(self):
        self._shutdown_event.set()
        if self._monitoring_task:
            await self._monitoring_task
            self._monitoring_task = None
        logger.info("Stopped Nomad job manager")

    # Retry decorator for Nomad API calls
    _nomad_retry = retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type(
            (
                nomad.api.exceptions.BaseNomadException,
                nomad.api.exceptions.URLNotFoundNomadException,
            )
        ),
    )

    @_nomad_retry
    async def _nomad_call(self, func, *args, **kwargs):
        """Execute a Nomad API call with retry logic."""
        return await asyncio.to_thread(func, *args, **kwargs)

    async def dispatch_and_track(
        self,
        job_name: str,
        prefix: str,
        meta: Optional[Dict[str, str]] = None,
        pipeline_id: Optional[str] = None,
    ) -> Tuple[str, int]:
        """
        Dispatch a job and track it to completion.

        Returns:
            Tuple of (job_id, exit_code)
        """
        job_id = await self._dispatch_job(job_name, prefix, meta)

        tracker = JobTracker(
            job_id=job_id,
            pipeline_id=pipeline_id or "unknown",
            task_name=job_name,
            stage=meta.get("stage") if meta else None,
        )
        self._active_jobs[job_id] = tracker

        # Update database if available
        await self._update_job_status(tracker)

        try:
            await self._wait_for_allocation(tracker)

            await self._wait_for_completion(tracker)

            if tracker.error:
                raise tracker.error

            return job_id, tracker.exit_code or 0

        finally:
            self._active_jobs.pop(job_id, None)

    async def _dispatch_job(
        self,
        job_name: str,
        prefix: str,
        meta: Optional[Dict[str, str]] = None,
    ) -> str:
        """Dispatch a parameterized job."""
        payload = {"Meta": meta} if meta else {}

        try:
            logger.debug(f"Dispatching job {job_name} with meta: {meta}")
            result = await self._nomad_call(
                self.client.job.dispatch_job,
                id_=job_name,
                payload=None,
                meta=meta,
                id_prefix_template=prefix,
            )
            job_id = result["DispatchedJobID"]
            logger.info(f"Dispatched job {job_id} from {job_name}")
            return job_id

        except Exception as e:
            logger.error(f"Failed to dispatch job {job_name}: {e}")
            logger.error(f"Payload was: {payload}")
            raise JobDispatchError(f"Failed to dispatch job {job_name}: {e}")

    async def _wait_for_allocation(self, tracker: JobTracker):
        while True:
            if tracker.allocation_id:
                return
            if tracker.status in (JobStatus.FAILED, JobStatus.LOST):
                raise JobDispatchError(f"Job {tracker.job_id} failed during allocation")
            await asyncio.sleep(1)

    async def _wait_for_completion(self, tracker: JobTracker):
        await tracker.completion_event.wait()

    async def _monitor_events(self):
        """Monitor Nomad event stream for job updates."""
        retry_count = 0
        max_retries = 5

        while not self._shutdown_event.is_set():
            try:
                await self._process_event_stream()
                retry_count = 0
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    logger.error(f"Event stream failed {max_retries} times, stopping monitor")
                    break

                wait_time = min(2**retry_count, 60)
                logger.warning(f"Event stream error: {e}, retrying in {wait_time}s")
                await asyncio.sleep(wait_time)

    async def _process_event_stream(self):
        if not self.session:
            self.session = aiohttp.ClientSession()

        url = f"{self.nomad_addr}/v1/event/stream"
        params = {
            "index": self._event_index,
            "namespace": self.namespace,
        }

        async with self.session.get(url, params=params, timeout=None) as response:
            response.raise_for_status()

            buffer = b""
            async for chunk in response.content.iter_chunked(8 * 1024):
                if self._shutdown_event.is_set():
                    break

                buffer += chunk
                while b"\n" in buffer:
                    line_bytes, buffer = buffer.split(b"\n", 1)
                    line = line_bytes.decode("utf-8").strip()

                    if not line or line == "{}":
                        continue

                    try:
                        data = json.loads(line)
                        if "Index" in data:
                            self._event_index = data["Index"]

                        events = data.get("Events", [])
                        for event in events:
                            await self._handle_event(event)

                    except json.JSONDecodeError:
                        logger.debug(f"Failed to parse event line: {line}")
                    except Exception as e:
                        logger.error(f"Error handling event: {e}")

    async def _handle_event(self, event: Dict[str, Any]):
        """Handle a single Nomad event."""
        topic = event.get("Topic")
        if topic != "Allocation":
            return

        payload = event.get("Payload", {}).get("Allocation", {})
        job_id = payload.get("JobID")

        if not job_id or job_id not in self._active_jobs:
            return

        tracker = self._active_jobs[job_id]
        allocation_id = payload.get("ID")
        client_status = payload.get("ClientStatus", "").lower()

        # Update allocation ID
        if not tracker.allocation_id and allocation_id:
            tracker.allocation_id = allocation_id
            logger.info(f"Job {job_id} allocated: {allocation_id}")

        # Map status
        old_status = tracker.status
        new_status = NOMAD_STATUS_MAP.get(client_status, JobStatus.UNKNOWN)

        if new_status != JobStatus.UNKNOWN and new_status != old_status:
            tracker.status = new_status
            tracker.timestamp = datetime.now(timezone.utc)

            # Extract exit code for completed jobs
            if client_status == "complete":
                task_states = payload.get("TaskStates", {})
                for task_state in task_states.values():
                    if task_state.get("FinishedAt"):
                        tracker.exit_code = task_state.get("ExitCode", 0)
                        break

            # Update database
            await self._update_job_status(tracker)

            if new_status in (JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.LOST):
                tracker.completion_event.set()
                logger.info(f"Job {job_id} completed with status {new_status}")

    async def _update_job_status(self, tracker: JobTracker):
        if not self.log_db:
            return

        try:
            status_str = tracker.status.value

            # Map internal status to database status if needed
            if tracker.status == JobStatus.ALLOCATED:
                status_str = "allocated"
            elif tracker.status == JobStatus.SUCCEEDED:
                status_str = "succeeded"

            await self.log_db.update_job_status(
                job_id=tracker.job_id,
                pipeline_id=tracker.pipeline_id,
                status=status_str,
                stage=tracker.stage or "unknown",
            )

        except Exception as e:
            logger.error(f"Failed to update job status in database: {e}")

    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        if job_id in self._active_jobs:
            tracker = self._active_jobs[job_id]
            return {
                "job_id": job_id,
                "status": tracker.status.value,
                "allocation_id": tracker.allocation_id,
                "exit_code": tracker.exit_code,
                "dispatch_time": tracker.dispatch_time,
            }

        # Query Nomad for job status
        try:
            job = await self._nomad_call(self.client.job.get_job, job_id)
            status = job.get("Status", "unknown").lower()
            return {
                "job_id": job_id,
                "status": NOMAD_STATUS_MAP.get(status, JobStatus.UNKNOWN).value,
            }
        except Exception as e:
            logger.error(f"Failed to get job status: {e}")
            raise JobNotFoundError(f"Job {job_id} not found")
