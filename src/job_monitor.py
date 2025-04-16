import asyncio
import logging
from typing import Dict, Optional
from contextlib import suppress
import aiohttp  # For exception types

# Use models and components from other modules
from nomad_api import NomadApiClient

# No custom exceptions imported


class NomadJobMonitor:
    """Listens to Nomad events and resolves Futures for tracked jobs."""

    def __init__(self, nomad_client: NomadApiClient):
        self._nomad_client = nomad_client
        self._job_futures: Dict[str, asyncio.Future] = {}
        self._future_metadata: Dict[str, Dict] = {}
        self._monitor_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self._active = True

    async def start(self):
        """Starts the background monitoring task."""
        # (Implementation remains the same)
        async with self._lock:
            if self._active and (
                self._monitor_task is None or self._monitor_task.done()
            ):
                logging.info("Starting Nomad job monitor task...")
                self._stop_event.clear()
                self._monitor_task = asyncio.create_task(
                    self._run_monitor(), name="NomadJobMonitorTask"
                )

    async def stop(self):
        """Stops the background monitoring task and cancels pending futures."""
        # (Implementation remains the same)
        async with self._lock:
            if not self._active:
                return
            self._active = False
            logging.info("Stopping Nomad job monitor...")
            if self._monitor_task and not self._monitor_task.done():
                self._stop_event.set()
                with suppress(asyncio.TimeoutError, asyncio.CancelledError):
                    await asyncio.wait_for(self._monitor_task, timeout=5.0)
                if not self._monitor_task.done():
                    logging.warning("Monitor task did not stop gracefully, cancelling.")
                    self._monitor_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await self._monitor_task
            pending_futures = []
            for job_id, future in self._job_futures.items():
                if not future.done():
                    logging.warning(
                        f"Cancelling pending future for job {job_id} due to monitor stop."
                    )
                    future.cancel("Monitor stopping")
                    pending_futures.append(job_id)
            self._job_futures.clear()
            self._future_metadata.clear()
            logging.info("Nomad job monitor stopped.")

    async def track_job(self, dispatched_job_id: str, meta: Dict) -> asyncio.Future:
        """Registers a job ID to be monitored, returning a Future for its completion."""
        # (Implementation remains the same)
        if not dispatched_job_id:
            raise ValueError("dispatched_job_id cannot be empty")
        if not self._active:
            raise RuntimeError("Job monitor is stopped.")
        async with self._lock:
            if self._monitor_task is None or self._monitor_task.done():
                await self.start()
            if dispatched_job_id in self._job_futures:
                logging.warning(f"Job {dispatched_job_id} is already being tracked.")
                return self._job_futures[dispatched_job_id]
            else:
                future = asyncio.get_running_loop().create_future()
                self._job_futures[dispatched_job_id] = future
                self._future_metadata[dispatched_job_id] = meta
                logging.info(f"Tracking job {dispatched_job_id}")
                return future

    def _extract_relevant_info(self, event: Dict) -> Optional[Dict]:
        """Parses Nomad event (primarily AllocationUpdate) for job status."""
        # (Implementation remains the same)
        event_type = event.get("Type")
        payload = event.get("Payload", {})
        if event_type == "AllocationUpdate":
            alloc = payload.get("Allocation")
            job = alloc.get("Job", {}) if alloc else {}
            if not alloc:
                return None
            job_id = alloc.get("JobID")
            meta = job.get("Meta")
            client_status = alloc.get("ClientStatus")
            task_states = alloc.get("TaskStates", {})
            failed = False
            all_tasks_terminal = len(task_states) > 0
            exit_code = None
            for state in task_states.values():
                task_state = state.get("State")
                if task_state not in ["dead", "complete"]:
                    all_tasks_terminal = False
                    break
                if state.get("Failed", False):
                    failed = True
                task_events = state.get("Events", [])
                if task_events and task_events[-1].get("Type") == "Terminated":
                    task_exit_code = task_events[-1].get("ExitCode")
                    if task_exit_code is not None:
                        exit_code = task_exit_code
                        if task_exit_code != 0:
                            failed = True
            status = "unknown"
            if all_tasks_terminal:
                status = "failed" if failed else "completed"
            elif client_status in ["running", "pending", "starting", "restarting"]:
                status = client_status
            output_path = meta.get("expected_output_path") if meta else None
            return {
                "job_id": job_id,
                "meta": meta,
                "status": status,
                "output_path": output_path,
                "exit_code": exit_code,
            }
        return None

    async def _run_monitor(self):
        """Background task listening to events and resolving futures."""
        # (Implementation remains the same)
        retry_delay = 1
        max_retry_delay = 60
        while not self._stop_event.is_set():
            task_name = (
                asyncio.current_task().get_name()
                if asyncio.current_task()
                else "UnknownMonitorTask"
            )
            try:
                async with self._nomad_client.events.stream() as stream_generator:
                    logging.info(f"Nomad event stream connected ({task_name}).")
                    retry_delay = 1
                    async for event in stream_generator:
                        if self._stop_event.is_set():
                            break
                        info = self._extract_relevant_info(event)
                        if info and info.get("job_id"):
                            await self._process_job_event(info)
                    if self._stop_event.is_set():
                        break
            except asyncio.CancelledError:
                logging.info(f"Monitor task ({task_name}) cancelled.")
                break
            except aiohttp.ClientError as e:
                logging.warning(f"Monitor stream connection error ({task_name}): {e}")
            except Exception:
                logging.exception(f"Unexpected error in monitor task ({task_name})")
            if self._stop_event.is_set():
                break
            logging.info(
                f"Retrying monitor stream connection in {retry_delay} seconds... ({task_name})"
            )
            with suppress(asyncio.CancelledError):
                await asyncio.shield(asyncio.sleep(retry_delay))
            retry_delay = min(retry_delay * 2, max_retry_delay)
        logging.info(f"Monitor task ({task_name}) finished.")

    async def _process_job_event(self, event_info: Dict):
        """Finds the future for the job ID and resolves it based on status."""
        dispatched_job_id = event_info.get("job_id")
        status = event_info.get("status")

        async with self._lock:
            future = self._job_futures.get(dispatched_job_id)
            original_meta = self._future_metadata.get(dispatched_job_id)

            if future and not future.done():
                logging.debug(
                    f"Processing event for tracked job {dispatched_job_id}: status={status}"
                )
                if status == "completed":
                    output_path = (
                        original_meta.get("expected_output_path")
                        if original_meta
                        else None
                    )
                    if not output_path:
                        logging.error(
                            f"Completed job {dispatched_job_id} missing 'expected_output_path' in original meta!"
                        )
                        # Use ValueError for missing data configuration issue
                        future.set_exception(
                            ValueError(
                                f"Output path missing for completed job {dispatched_job_id}"
                            )
                        )
                    else:
                        logging.info(f"Job {dispatched_job_id} completed successfully.")
                        future.set_result(output_path)
                    # Clean up tracking info for this job
                    if dispatched_job_id in self._job_futures:
                        del self._job_futures[dispatched_job_id]
                    if dispatched_job_id in self._future_metadata:
                        del self._future_metadata[dispatched_job_id]

                elif status == "failed":
                    exit_code = event_info.get("exit_code", "N/A")
                    err_msg = f"Job {dispatched_job_id} failed (exit code: {exit_code})"
                    logging.error(err_msg)
                    # Use RuntimeError for job execution failures
                    future.set_exception(RuntimeError(err_msg))
                    # Clean up tracking info
                    if dispatched_job_id in self._job_futures:
                        del self._job_futures[dispatched_job_id]
                    if dispatched_job_id in self._future_metadata:
                        del self._future_metadata[dispatched_job_id]
                # else: status is running, pending etc. - future remains pending
