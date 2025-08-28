import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import nomad
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


class JobStatus(Enum):
    """Internal job status enumeration aligned with Nomad ClientStatus values."""

    DISPATCHED = "dispatched"
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    LOST = "lost"
    STOPPED = "stopped"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


# Mapping from Nomad statuses to our internal statuses
NOMAD_STATUS_MAP = {
    "pending": JobStatus.PENDING,
    "running": JobStatus.RUNNING,
    "complete": JobStatus.SUCCEEDED,
    "failed": JobStatus.FAILED,
    "lost": JobStatus.LOST,
    "dead": JobStatus.STOPPED,
    "stopped": JobStatus.STOPPED,
    "cancelled": JobStatus.CANCELLED,
}


class NomadError(Exception):
    """Base exception for the NomadJobManager."""

    pass


class JobNotFoundError(NomadError):
    """Raised when a specific job cannot be found in Nomad."""

    pass


class JobDispatchError(NomadError):
    """
    Raised when a job fails to dispatch or fails during execution.
    Contains enriched details about the failure.
    """

    def __init__(self, message: str, **kwargs):
        super().__init__(message)
        self.job_id: Optional[str] = kwargs.get("job_id")
        self.allocation_id: Optional[str] = kwargs.get("allocation_id")
        self.exit_code: Optional[int] = kwargs.get("exit_code")
        self.original_error: Optional[Exception] = kwargs.get("original_error")


@dataclass
class JobTracker:
    """Tracks the state of a single Nomad job."""

    job_id: str
    dispatch_time: float = field(default_factory=time.time)
    status: JobStatus = JobStatus.DISPATCHED
    allocation_id: Optional[str] = None
    exit_code: Optional[int] = None
    completion_event: asyncio.Event = field(default_factory=asyncio.Event)
    error: Optional[Exception] = None
    task_name: Optional[str] = None
    stage: Optional[str] = None
    timestamp: Optional[datetime] = None


class _NomadAPIClient:
    """A wrapper for Nomad API calls that provides retries and concurrency limiting."""

    def __init__(self, client: nomad.Nomad, max_concurrency: int):
        self.client = client
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self._nomad_retry = retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=2, max=30),
            retry=retry_if_exception_type(
                (
                    nomad.api.exceptions.BaseNomadException,
                    nomad.api.exceptions.URLNotFoundNomadException,
                )
            ),
        )

    async def _call(self, func: Callable, *args, **kwargs) -> Any:
        async with self.semaphore:
            return await asyncio.to_thread(func, *args, **kwargs)

    async def dispatch_job(
        self, job_name: str, prefix: str, meta: Optional[Dict[str, str]]
    ) -> str:
        wrapped_call = self._nomad_retry(self._call)
        result = await wrapped_call(
            self.client.job.dispatch_job,
            id_=job_name,
            payload=None,
            meta=meta,
            id_prefix_template=prefix.replace(":", "-"),
        )
        return result["DispatchedJobID"]

    async def get_job(self, job_id: str) -> Dict[str, Any]:
        wrapped_call = self._nomad_retry(self._call)
        return await wrapped_call(self.client.job.get_job, job_id)

    async def get_allocations(self, job_id: str) -> List[Dict[str, Any]]:
        wrapped_call = self._nomad_retry(self._call)
        return await wrapped_call(self.client.job.get_allocations, job_id)


class NomadJobManager:
    """
    Manages the lifecycle of parameterized Nomad jobs using a polling-based
    strategy with exponential backoff.
    """

    def __init__(
        self,
        nomad_addr: str,
        namespace: str = "default",
        token: Optional[str] = None,
        log_db: Optional[Any] = None,
        max_concurrent_dispatch: int = 10,
    ):
        self.nomad_addr = nomad_addr
        self.namespace = namespace
        self.token = token
        self.log_db = log_db

        parsed = urlparse(nomad_addr)
        nomad_client = nomad.Nomad(
            host=parsed.hostname,
            port=parsed.port,
            verify=False,
            token=token,
            namespace=namespace,
        )
        self.api = _NomadAPIClient(nomad_client, max_concurrent_dispatch)
        self._active_jobs: Dict[str, JobTracker] = {}
        self._shutdown_event = asyncio.Event()

    async def start(self):
        """Starts the job manager (no background tasks in this version)."""
        logger.info("Starting NomadJobManager (polling-based).")
        # No background tasks to start in a pure polling model

    async def stop(self):
        """Stops the job manager."""
        logger.info("Stopping NomadJobManager.")
        self._shutdown_event.set()
        # No background tasks to stop

    async def dispatch_and_track(
        self,
        job_name: str,
        prefix: str,
        meta: Optional[Dict[str, str]] = None,
    ) -> Tuple[str, int]:
        """
        Dispatches a job and polls its status until completion using
        an exponential backoff strategy.
        """
        try:
            job_id = await self.api.dispatch_job(job_name, prefix, meta)
        except Exception as e:
            # Extract more detailed error information
            error_msg = str(e)

            # Special handling for RetryError that wraps BaseNomadException
            if (
                "RetryError" in str(type(e))
                and "BaseNomadException" in error_msg
            ):
                # Try to extract the cause from RetryError
                if hasattr(e, "__cause__") and e.__cause__:
                    error_msg = f"{error_msg} - Cause: {str(e.__cause__)}"
                elif hasattr(e, "last_attempt") and hasattr(
                    e.last_attempt, "exception"
                ):
                    # For tenacity RetryError
                    last_exception = e.last_attempt.exception()
                    if last_exception:
                        error_msg = f"RetryError after 3 attempts - Last error: {str(last_exception)}"

            logger.error(f"Failed to dispatch job {job_name}: {error_msg}")
            raise JobDispatchError(
                f"Failed to dispatch job {job_name}: {error_msg}"
            ) from e

        tracker = JobTracker(
            job_id=job_id, task_name=job_name, stage=(meta or {}).get("stage")
        )
        self._active_jobs[job_id] = tracker
        await self._update_db_status(tracker)

        try:
            # Polling loop with exponential backoff
            poll_delay = 1.0  # Start with a 1-second delay
            backoff_factor = 1.5
            jitter_factor = 0.25  # Apply up to 25% jitter
            max_poll_delay = 30.0  # Max delay of 30s

            while not tracker.completion_event.is_set():
                jitter = poll_delay * jitter_factor * random.uniform(-1, 1)
                await asyncio.sleep(max(0, poll_delay + jitter))

                await self._poll_job_and_update_tracker(tracker)

                # Increase delay for the next poll
                poll_delay = min(poll_delay * backoff_factor, max_poll_delay)

            if tracker.error:
                raise JobDispatchError(
                    f"Job {job_id} failed with exit code {tracker.exit_code}: {tracker.error}",
                    job_id=job_id,
                    allocation_id=tracker.allocation_id,
                    exit_code=tracker.exit_code,
                    original_error=tracker.error,
                )
            return job_id, tracker.exit_code or 0
        finally:
            self._active_jobs.pop(job_id, None)

    async def _poll_job_and_update_tracker(self, tracker: JobTracker):
        """Polls a single job and updates its tracker if the status has changed."""
        try:
            # First, check if the job itself still exists. This avoids polling for allocations of a job that has been purged.
            await self.api.get_job(tracker.job_id)

            allocations = await self.api.get_allocations(tracker.job_id)

            time_since_dispatch = time.time() - tracker.dispatch_time
            if (
                not allocations and time_since_dispatch > 28800
            ):  # allow 8 hrs to allocate for jobs that take a while to go from pending to dispatched
                logger.warning(
                    f"No allocations found for job {tracker.job_id} after 30 minutes, marking as LOST."
                )
                tracker.status = JobStatus.LOST
                tracker.error = Exception(
                    "Job lost: No allocations found after timeout."
                )
                tracker.completion_event.set()
                await self._update_db_status(tracker)
                return

            if not allocations:
                logger.debug(f"Polling {tracker.job_id}: No allocations yet.")
                return

            latest_alloc = max(
                allocations, key=lambda a: a.get("CreateTime", 0)
            )
            await self._update_tracker_from_allocation(tracker, latest_alloc)

        except (
            nomad.api.exceptions.URLNotFoundNomadException,
            nomad.api.exceptions.BaseNomadException,
        ) as e:
            # Extract more detailed error information
            error_msg = str(e)
            if hasattr(e, "__cause__") and e.__cause__:
                error_msg = f"{error_msg} - Cause: {str(e.__cause__)}"

            logger.error(
                f"Polling failed for job {tracker.job_id}, it may have been purged. Marking as LOST. Error: {error_msg}"
            )
            tracker.status = JobStatus.LOST
            tracker.error = e
            tracker.completion_event.set()
            await self._update_db_status(tracker)
        except Exception as e:
            # Check if this is a RetryError wrapping a Nomad exception
            if "RetryError" in str(type(e)):
                # Try to extract the wrapped exception
                wrapped_exception = None
                if hasattr(e, "last_attempt") and hasattr(e.last_attempt, "exception"):
                    wrapped_exception = e.last_attempt.exception()
                elif hasattr(e, "__cause__") and e.__cause__:
                    wrapped_exception = e.__cause__
                
                # Check if the wrapped exception is a URLNotFoundNomadException
                if wrapped_exception and "URLNotFoundNomadException" in str(type(wrapped_exception)):
                    logger.error(
                        f"Polling failed for job {tracker.job_id} after retries, job may have been purged. Marking as LOST. Error: {e}"
                    )
                    tracker.status = JobStatus.LOST
                    tracker.error = e
                    tracker.completion_event.set()
                    await self._update_db_status(tracker)
                    return
            
            logger.warning(
                f"Unexpected error while polling for job {tracker.job_id}: {e}"
            )

    async def _update_tracker_from_allocation(
        self, tracker: JobTracker, allocation: Dict[str, Any]
    ):
        """Updates a tracker's state based on a Nomad allocation object."""
        if tracker.completion_event.is_set():
            return

        old_status = tracker.status
        client_status = allocation.get("ClientStatus", "").lower()
        new_status = NOMAD_STATUS_MAP.get(client_status, JobStatus.UNKNOWN)

        if new_status == old_status:
            return  # No change

        logger.info(
            f"Job {tracker.job_id} status change: {old_status.name} -> {new_status.name}"
        )
        tracker.status = new_status
        tracker.timestamp = datetime.now(timezone.utc)
        if not tracker.allocation_id:
            tracker.allocation_id = allocation.get("ID")

        is_terminal = new_status in (
            JobStatus.SUCCEEDED,
            JobStatus.FAILED,
            JobStatus.LOST,
            JobStatus.STOPPED,
            JobStatus.CANCELLED,
        )

        if is_terminal:
            self._extract_final_state(tracker, allocation)
            logger.info(
                f"Job {tracker.job_id} completed with status JobStatus.{new_status.name} "
                f"(exit_code: {tracker.exit_code})"
            )
            tracker.completion_event.set()

        await self._update_db_status(tracker)

    def _extract_final_state(
        self, tracker: JobTracker, allocation: Dict[str, Any]
    ):
        """Extracts the exit code and error message from a terminal allocation."""
        task_states = allocation.get("TaskStates", {})
        for task_name, task_state in task_states.items():
            if task_state.get("FinishedAt"):
                if tracker.status == JobStatus.FAILED:
                    tracker.exit_code = task_state.get("ExitCode", 1)
                    failure_details = []
                    # Search for a meaningful event message
                    for event in reversed(task_state.get("Events", [])):
                        if event.get("Type") in [
                            "Terminated",
                            "Task Failed",
                            "Driver Failure",
                            "Killing",
                        ]:
                            reason = event.get(
                                "DisplayMessage", "No reason provided."
                            )
                            failure_details.append(
                                f"Task '{task_name}' failed: {reason}"
                            )
                            break
                    if not failure_details:
                        failure_details.append(
                            f"Task '{task_name}' failed with exit code {tracker.exit_code}."
                        )
                    tracker.error = Exception("; ".join(failure_details))
                elif tracker.status == JobStatus.CANCELLED:
                    tracker.exit_code = task_state.get("ExitCode", 0)
                    # Search for cancellation reason
                    cancellation_reason = "Job was cancelled"
                    for event in reversed(task_state.get("Events", [])):
                        if event.get("Type") in ["Killing", "Terminated"]:
                            reason = event.get("DisplayMessage", "")
                            if reason:
                                cancellation_reason = f"Job cancelled: {reason}"
                                break
                    tracker.error = Exception(cancellation_reason)
                elif tracker.status == JobStatus.STOPPED:
                    tracker.exit_code = task_state.get("ExitCode", 0)
                    # Search for stop reason
                    stop_reason = "Job was stopped"
                    for event in reversed(task_state.get("Events", [])):
                        if event.get("Type") in [
                            "Killing",
                            "Terminated",
                            "Not Restarting",
                        ]:
                            reason = event.get("DisplayMessage", "")
                            if reason:
                                stop_reason = f"Job stopped: {reason}"
                                break
                    tracker.error = Exception(stop_reason)
                else:
                    tracker.exit_code = task_state.get("ExitCode", 0)
                return

    async def _update_db_status(self, tracker: JobTracker):
        """Updates an external database with the latest job status."""
        logger.info(
            f"Job {tracker.job_id} status updated: JobStatus.{tracker.status.name}"
        )
        if not self.log_db:
            return
        try:
            await self.log_db.update_job_status(
                job_id=tracker.job_id,
                status=tracker.status.value,
                stage=tracker.stage or "unknown",
            )
        except Exception as e:
            logger.error(
                f"Failed to update job status in DB for {tracker.job_id}: {e}"
            )
