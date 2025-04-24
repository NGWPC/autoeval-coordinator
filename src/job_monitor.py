import asyncio
import logging
from typing import Dict, Optional
from contextlib import suppress
import aiohttp

from nomad_api import NomadApiClient


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
            for job_id, future in list(self._job_futures.items()):
                if not future.done():
                    logging.warning(
                        f"Cancelling pending future for job {job_id} due to monitor stop."
                    )
                    future.set_exception(RuntimeError("Monitor stopping"))
                    pending_futures.append(job_id)
            self._job_futures.clear()
            self._future_metadata.clear()
            logging.info("Nomad job monitor stopped.")

    async def track_job(self, dispatched_job_id: str, meta: Dict) -> asyncio.Future:
        """Registers a job ID to be monitored, returning a Future for its completion."""
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
                logging.info(f"Tracking job {dispatched_job_id} with meta: {meta}")
                return future

    def _extract_relevant_info(self, event: Dict) -> Optional[Dict]:
        """
        Parses Nomad AllocationUpdated event for job status, prioritizing ClientStatus.
        """
        event_type = event.get("Type")
        payload = event.get("Payload", {})

        if event_type != "AllocationUpdated":
            return None

        alloc = payload.get("Allocation")
        if not alloc:
            logging.debug("AllocationUpdated event missing Allocation payload")
            return None

        job_id = alloc.get("JobID")
        if not job_id:
            logging.debug("AllocationUpdated event missing JobID in Allocation")
            return None

        client_status = alloc.get("ClientStatus")
        task_states = alloc.get("TaskStates", {})

        status = "unknown"
        exit_code = None
        failed = False

        # --- Check definitive ClientStatus values ---
        if client_status == "complete":
            status = "completed"
            failed = False  # Default assumption
            exit_code = 0  # Default assumption

            # Double-check TaskStates for any failures, even if ClientStatus is complete
            if task_states:
                for task_name, state in task_states.items():
                    task_failed = state.get("Failed", False)
                    task_exit_code = None
                    task_events = state.get("Events", [])
                    term_events = [
                        e for e in task_events if e.get("Type") == "Terminated"
                    ]
                    if term_events:
                        last_term_event = term_events[-1]
                        # Attempt to get exit code as int, handle potential string "0"
                        try:
                            task_exit_code_str = last_term_event.get("Details", {}).get(
                                "exit_code"
                            )
                            if task_exit_code_str is not None:
                                task_exit_code = int(task_exit_code_str)
                            else:
                                # Fallback if 'exit_code' key missing in Details
                                task_exit_code = last_term_event.get(
                                    "ExitCode"
                                )  # Check root level too
                        except (ValueError, TypeError):
                            logging.warning(
                                f"Job {job_id}: Could not parse exit_code '{task_exit_code_str}' as int for task '{task_name}'."
                            )
                            task_exit_code = None  # Treat unparseable as unknown

                    if task_failed or (
                        task_exit_code is not None and task_exit_code != 0
                    ):
                        failed = True
                        status = "failed"  # Override status if any task failed
                        exit_code = (
                            task_exit_code if task_exit_code is not None else -1
                        )  # Capture non-zero or indicate failure
                        logging.warning(
                            f"Job {job_id}: ClientStatus 'complete' but task '{task_name}' indicates failure (Failed: {task_failed}, ExitCode: {task_exit_code}). Setting status to 'failed'."
                        )
                        break  # One failed task is enough to mark the job failed

        elif client_status in ["failed", "lost"]:
            status = "failed"
            failed = True
            exit_code = None  # Often unknown in these cases, but try to find one
            if task_states:
                for task_name, state in task_states.items():
                    task_events = state.get("Events", [])
                    term_events = [
                        e for e in task_events if e.get("Type") == "Terminated"
                    ]
                    if term_events:
                        last_term_event = term_events[-1]
                        # Attempt to get exit code as int
                        try:
                            task_exit_code_str = last_term_event.get("Details", {}).get(
                                "exit_code"
                            )
                            if task_exit_code_str is not None:
                                task_exit_code = int(task_exit_code_str)
                            else:
                                task_exit_code = last_term_event.get("ExitCode")
                        except (ValueError, TypeError):
                            logging.warning(
                                f"Job {job_id}: Could not parse exit_code '{task_exit_code_str}' as int for failed task '{task_name}'."
                            )
                            task_exit_code = None

                        if task_exit_code is not None:
                            exit_code = (
                                task_exit_code  # Record the first found exit code
                            )
                            break  # Stop checking other tasks

        # --- ClientStatus is ambiguous, check TaskStates ---
        elif task_states:
            all_tasks_terminal = True
            any_task_failed = False
            final_exit_code = (
                0  # Default to 0 for success if all terminal and none failed
            )

            for task_name, state in task_states.items():
                task_state = state.get("State")
                task_failed_flag = state.get("Failed", False)
                task_term_exit_code = None

                if task_state not in ["dead", "complete"]:
                    all_tasks_terminal = False
                    break  # If any task isn't terminal, the whole allocation isn't terminal yet

                # Check for failure condition in this terminal task
                task_events = state.get("Events", [])
                term_events = [e for e in task_events if e.get("Type") == "Terminated"]
                if term_events:
                    last_term_event = term_events[-1]
                    # Attempt to get exit code as int
                    try:
                        task_exit_code_str = last_term_event.get("Details", {}).get(
                            "exit_code"
                        )
                        if task_exit_code_str is not None:
                            task_term_exit_code = int(task_exit_code_str)
                        else:
                            task_term_exit_code = last_term_event.get("ExitCode")
                    except (ValueError, TypeError):
                        logging.warning(
                            f"Job {job_id}: Could not parse exit_code '{task_exit_code_str}' as int for terminal task '{task_name}'."
                        )
                        task_term_exit_code = None

                if task_failed_flag or (
                    task_term_exit_code is not None and task_term_exit_code != 0
                ):
                    any_task_failed = True
                    # Capture the first non-zero exit code as the overall exit code
                    if final_exit_code == 0 and task_term_exit_code is not None:
                        final_exit_code = task_term_exit_code
                    elif (
                        final_exit_code == 0
                    ):  # Task failed flag but exit code was 0 or missing
                        final_exit_code = -1  # Indicate failure generically

            # Determine status based on task analysis
            if all_tasks_terminal:
                if any_task_failed:
                    status = "failed"
                    exit_code = final_exit_code
                else:
                    status = "completed"
                    exit_code = 0
            else:
                # Not all tasks are terminal, use ClientStatus if it's an active one
                if client_status in ["running", "pending", "starting", "restarting"]:
                    status = client_status
                else:
                    # Default to unknown if ClientStatus isn't a recognizable active state
                    status = "unknown"
                exit_code = None

        # --- Fallback (ClientStatus ambiguous/missing, no TaskStates) ---
        else:
            # Use ClientStatus if it's a known active state, otherwise unknown
            if client_status in ["running", "pending", "starting", "restarting"]:
                status = client_status
            else:
                status = "unknown"  # Default if no other info available
            exit_code = None

        # --- Construct Result ---
        # Note: event_output_path is removed as the pipeline should use the originally tracked metadata
        return {
            "job_id": job_id,
            "status": status,
            "exit_code": exit_code,
        }

    async def _run_monitor(self):
        """Background task listening to events and resolving futures."""
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
                            logging.debug(
                                f"Stop event set, breaking event loop ({task_name})"
                            )
                            break
                        try:
                            info = self._extract_relevant_info(event)
                            if info and info.get("job_id"):
                                await self._process_job_event(info)
                        except Exception as e:
                            logging.exception(
                                f"Error processing event ({task_name}): {event}. Error: {e}"
                            )

                    if self._stop_event.is_set():
                        logging.debug(
                            f"Stop event set after stream ended ({task_name})"
                        )
                        break  # Exit while loop if stopped during stream processing
                    else:
                        logging.warning(
                            f"Nomad event stream ended unexpectedly ({task_name})."
                        )

            except asyncio.CancelledError:
                logging.info(f"Monitor task ({task_name}) cancelled.")
                break
            except aiohttp.ClientError as e:
                logging.warning(f"Monitor stream connection error ({task_name}): {e}")
            except Exception:
                logging.exception(f"Unexpected error in monitor task ({task_name})")
            if self._stop_event.is_set():
                logging.debug(f"Stop event set before retry sleep ({task_name})")
                break  # Exit while loop if stopped

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
                        original_meta.get("output_path") if original_meta else None
                    )

                    if not output_path:
                        logging.error(
                            f"Completed job {dispatched_job_id} missing 'output_path' in original tracked meta! "
                            f"Original Meta: {original_meta}. Event Info: {event_info}"
                        )
                        # Use ValueError for missing *required configuration* data
                        future.set_exception(
                            ValueError(
                                f"Output path missing in tracked metadata for completed job {dispatched_job_id}"
                            )
                        )
                    else:
                        logging.info(
                            f"Job {dispatched_job_id} completed successfully. Result: {output_path}"
                        )
                        future.set_result(output_path)

                    # Clean up tracking info once resolved (success or config error)
                    if dispatched_job_id in self._job_futures:
                        del self._job_futures[dispatched_job_id]
                    if dispatched_job_id in self._future_metadata:
                        del self._future_metadata[dispatched_job_id]

                elif status == "failed":
                    exit_code = event_info.get("exit_code", "N/A")
                    err_msg = f"Job {dispatched_job_id} failed (exit code: {exit_code})"
                    logging.error(err_msg + f". Event Info: {event_info}")
                    # Use RuntimeError for job execution failures
                    future.set_exception(RuntimeError(err_msg))

                    # Clean up tracking info for failed jobs
                    if dispatched_job_id in self._job_futures:
                        del self._job_futures[dispatched_job_id]
                    if dispatched_job_id in self._future_metadata:
                        del self._future_metadata[dispatched_job_id]
                # else: status is running, pending etc. - future remains pending
