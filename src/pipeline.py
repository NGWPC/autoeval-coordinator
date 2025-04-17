import asyncio
import logging
import os
from enum import Enum
from typing import Dict, Optional, Any, List, Tuple
from contextlib import suppress

from load_config import AppConfig
from nomad_api import NomadApiClient
from job_monitor import NomadJobMonitor
from data_service import DataService
import pipeline_params


class PipelineState(Enum):
    INITIALIZING = "init"
    DISPATCHING_INUNDATORS = "dispatch_inund"
    AWAITING_INUNDATORS = "await_inund"
    DISPATCHING_MOSAICKER = "dispatch_mosaic"
    AWAITING_MOSAICKER = "await_mosaic"
    COMPLETED = "completed"
    FAILED = "failed"


class PolygonPipeline:
    """Orchestrates the steps for processing a single polygon."""

    def __init__(
        self,
        pipeline_id: str,
        polygon_data: Dict,
        config: AppConfig,
        nomad_client: NomadApiClient,
        data_service: DataService,
        job_monitor: NomadJobMonitor,
    ):
        self.pipeline_id = pipeline_id
        self.instance_id = f"{pipeline_id}"
        self.polygon_data = polygon_data
        self.config = config
        self.nomad_client = nomad_client
        self.data_service = data_service
        self.job_monitor = job_monitor
        self.state = PipelineState.INITIALIZING
        self.catchment_data: Optional[Dict[str, Dict]] = None
        self.hand_version: Optional[str] = None
        self.temp_dir: Optional[str] = None
        self._final_result: Any = None
        self._failure_reason: Optional[BaseException] = None
        logging.info(f"Pipeline {self.pipeline_id} created")

    async def initialize(self) -> bool:
        self.state = PipelineState.INITIALIZING
        logging.info(f"[{self.pipeline_id}] Initializing...")
        try:
            full_data = await self.data_service.query_for_catchments(self.polygon_data)
            self.catchment_data = full_data.get("catchments")
            self.hand_version = full_data.get("hand_version")

            if not self.catchment_data:
                raise ValueError("No catchments found for polygon.")
            self.temp_dir = os.path.join(
                os.getcwd(), "temp_pipeline_data", self.pipeline_id
            )
            os.makedirs(self.temp_dir, exist_ok=True)
            logging.info(
                f"[{self.pipeline_id}] Found {len(self.catchment_data)} catchments. Temp dir: {self.temp_dir}"
            )
            return True
        except Exception as e:
            logging.exception(f"[{self.pipeline_id}] Initialization failed")
            self.state = PipelineState.FAILED
            self._failure_reason = e
            return False

    async def _validate_write_dispatch_track(
        self, catchment_id: str, c_info: Dict
    ) -> Tuple[str, asyncio.Future]:
        """
        Helper to validate, write JSON, dispatch job, and initiate tracking for one catchment.
        Ensures write completes before dispatch.
        Returns (dispatched_job_id, job_future).
        Raises exceptions on failure.
        """
        try:
            # 1. Validate c_info content
            if "raster_pair" not in c_info:
                raise KeyError("Missing 'raster_pair'")
            if "hydrotable_entries" not in c_info:
                raise KeyError(
                    "Missing 'hydrotable_entries'"
                )  # Or handle default if allowed
            raster_pair = c_info["raster_pair"]
            rem_raster_s3 = raster_pair.get("rem_raster_path")
            catch_raster_s3 = raster_pair.get("catchment_raster_path")
            if not rem_raster_s3 or not rem_raster_s3.startswith("s3://"):
                raise ValueError(f"Invalid rem_raster_path: {rem_raster_s3}")
            if not catch_raster_s3 or not catch_raster_s3.startswith("s3://"):
                raise ValueError(f"Invalid catch_raster_path: {catch_raster_s3}")

            # Use c_info directly as content after validation
            json_content_to_write = c_info

            # 2. Determine S3 Path for this JSON --- Corrected Indentation Starts Here
            input_json_s3_path = pipeline_params.generate_s3_path(
                self.config,
                self.pipeline_id,
                catchment_id=catchment_id,
                filename="catchment_data.json",
                is_input=True,
            )

            # 3. Write JSON to S3 (Await completion)
            logging.debug(
                f"[{self.pipeline_id}-{catchment_id}] Writing input JSON to {input_json_s3_path}"
            )
            await self.data_service.write_json_to_uri(
                json_content_to_write, input_json_s3_path
            )
            logging.info(
                f"[{self.pipeline_id}-{catchment_id}] Successfully wrote input JSON."
            )

            # 4. Prepare Dispatch Metadata (Now that write is done)
            dispatch_meta = pipeline_params.prepare_inundator_dispatch_info(
                self.config,
                self.pipeline_id,
                catchment_id,
            )
            # Sanity check path consistency
            if dispatch_meta.get("catchment_data_path") != input_json_s3_path:
                logging.error(
                    f"[{self.pipeline_id}-{catchment_id}] Mismatch in calculated input S3 path!"
                )
                raise RuntimeError(f"S3 path mismatch for {catchment_id}")

            # 5. Dispatch Job (Await API call response)
            job_name = self.config.jobs.hand_inundator
            instance_prefix = f"inundate-{self.pipeline_id[:8]}-{catchment_id}"
            logging.debug(
                f"[{self.pipeline_id}-{catchment_id}] Dispatching job '{job_name}' with prefix '{instance_prefix}'"
            )
            dispatch_response = await self.nomad_client.dispatch_job(
                job_name, instance_prefix, dispatch_meta
            )
            dispatched_job_id = dispatch_response.get("DispatchedJobID")
            if not dispatched_job_id:
                logging.error(
                    f"[{self.pipeline_id}-{catchment_id}] Dispatch failed, response missing DispatchedJobID: {dispatch_response}"
                )
                raise ValueError("Dispatch response missing DispatchedJobID.")
            logging.info(
                f"[{self.pipeline_id}-{catchment_id}] Job dispatched: {dispatched_job_id}"
            )

            # 6. Initiate Tracking (Await setup)
            logging.debug(
                f"[{self.pipeline_id}-{catchment_id}] Initiating tracking for job {dispatched_job_id}"
            )
            job_future = await self.job_monitor.track_job(
                dispatched_job_id, dispatch_meta
            )
            logging.info(
                f"[{self.pipeline_id}-{catchment_id}] Tracking initiated for job {dispatched_job_id}"
            )

            return dispatched_job_id, job_future

        except Exception as e:
            # Log exception with context before re-raising
            logging.exception(
                f"[{self.pipeline_id}-{catchment_id}] Failed during validate/write/dispatch/track."
            )
            # Re-raise the original exception to be caught by gather
            raise

    async def run(self):
        """Executes the pipeline steps: write inputs -> dispatch jobs -> await -> dispatch mosaicker -> await."""
        if self.state != PipelineState.INITIALIZING:
            logging.error(
                f"[{self.pipeline_id}] Cannot run, invalid state: {self.state}"
            )
            return
        if not self.catchment_data:
            self.state = PipelineState.FAILED
            self._failure_reason = ValueError(
                "Cannot run pipeline without catchment data."
            )
            logging.error(f"[{self.pipeline_id}] Cannot run without catchment data.")
            return

        logging.info(f"[{self.pipeline_id}] Running pipeline...")
        # Store futures keyed by dispatched job ID for later await
        inundator_futures: Dict[str, asyncio.Future] = {}
        # Store mapping from catchment ID to dispatched job ID
        dispatched_ids: Dict[str, str] = {}

        try:
            # --- Step 1: Process Catchments Concurrently (Validate -> Write -> Dispatch -> Track Start) ---
            self.state = PipelineState.DISPATCHING_INUNDATORS
            logging.info(
                f"[{self.pipeline_id}] Processing {len(self.catchment_data)} catchments concurrently (validate, write, dispatch, start tracking)..."
            )

            # Create coroutines for the helper function for each catchment
            processing_tasks = [
                self._validate_write_dispatch_track(catchment_id, c_info)
                for catchment_id, c_info in self.catchment_data.items()
            ]

            # Run the processing tasks concurrently
            results = await asyncio.gather(*processing_tasks, return_exceptions=True)

            # --- Process Results of Step 1 ---
            successful_dispatches = 0
            failed_catchments = []
            first_failure_exception: Optional[BaseException] = None
            catchment_ids_list = list(
                self.catchment_data.keys()
            )  # Order matches results

            for i, result in enumerate(results):
                catchment_id = catchment_ids_list[i]
                if isinstance(result, Exception):
                    # Helper already logged details
                    logging.error(
                        f"[{self.pipeline_id}] Processing failed for catchment {catchment_id}."
                    )
                    failed_catchments.append(catchment_id)
                    if first_failure_exception is None:
                        first_failure_exception = result
                elif isinstance(result, tuple) and len(result) == 2:
                    dispatched_job_id, job_future = result
                    inundator_futures[dispatched_job_id] = job_future
                    dispatched_ids[catchment_id] = dispatched_job_id
                    successful_dispatches += 1
                else:
                    # Should not happen
                    logging.error(
                        f"[{self.pipeline_id}] Unexpected result type for catchment {catchment_id}: {result}"
                    )
                    failed_catchments.append(catchment_id)
                    if first_failure_exception is None:
                        first_failure_exception = TypeError(
                            f"Unexpected result type for {catchment_id}"
                        )

            # Check if any processing failed
            if failed_catchments:
                logging.error(
                    f"[{self.pipeline_id}] Failed to process {len(failed_catchments)} catchments: {failed_catchments}. Aborting."
                )
                raise RuntimeError(
                    f"Pipeline aborted due to failures in {len(failed_catchments)} catchments during dispatch phase."
                ) from first_failure_exception

            if successful_dispatches == 0:
                logging.error(
                    f"[{self.pipeline_id}] No inundator jobs were successfully dispatched and tracked."
                )
                raise RuntimeError(
                    "No inundator jobs dispatched or tracked successfully."
                )

            logging.info(
                f"[{self.pipeline_id}] Successfully dispatched and initiated tracking for {successful_dispatches} inundator jobs."
            )

            # --- Step 2: Await Inundator Job Completion ---
            self.state = PipelineState.AWAITING_INUNDATORS
            logging.info(
                f"[{self.pipeline_id}] Awaiting completion of {len(inundator_futures)} inundator jobs..."
            )

            # Await all the futures collected in Step 1
            job_completion_results = await asyncio.gather(
                *inundator_futures.values(), return_exceptions=True
            )

            # --- Process Job Completion Results ---
            completed_outputs: List[str] = []
            failed_jobs: List[str] = []
            first_job_failure: Optional[BaseException] = None
            job_ids_list = list(inundator_futures.keys())  # Order matches results

            for i, job_result in enumerate(job_completion_results):
                dispatched_job_id = job_ids_list[i]
                # Find catchment_id using the dispatched_ids map created earlier
                catchment_id = next(
                    (
                        cid
                        for cid, d_id in dispatched_ids.items()
                        if d_id == dispatched_job_id
                    ),
                    "UNKNOWN_CATCHMENT",
                )

                if isinstance(job_result, BaseException):
                    logging.error(
                        f"[{self.pipeline_id}] Inundator job {dispatched_job_id} (Catchment: {catchment_id}) failed during execution: {job_result}"
                    )
                    failed_jobs.append(dispatched_job_id)
                    if first_job_failure is None:
                        first_job_failure = job_result
                elif (
                    job_result
                    and isinstance(job_result, str)
                    and job_result.startswith("s3://")
                ):
                    logging.info(
                        f"[{self.pipeline_id}] Inundator job {dispatched_job_id} (Catchment: {catchment_id}) succeeded. Output: {job_result}"
                    )
                    completed_outputs.append(str(job_result))
                else:
                    logging.error(
                        f"[{self.pipeline_id}] Inundator job {dispatched_job_id} (Catchment: {catchment_id}) finished with unexpected result: {job_result}"
                    )
                    failed_jobs.append(dispatched_job_id)
                    if first_job_failure is None:
                        first_job_failure = ValueError(
                            f"Job {dispatched_job_id} returned unexpected result: {job_result}"
                        )

            # Check for job failures or missing outputs
            expected_outputs = (
                successful_dispatches  # Number of jobs we successfully started tracking
            )
            if failed_jobs or len(completed_outputs) != expected_outputs:
                logging.error(
                    f"[{self.pipeline_id}] {len(failed_jobs)} inundator jobs failed, and {len(completed_outputs)} completed successfully (expected {expected_outputs})."
                )
                raise first_job_failure or RuntimeError(
                    f"Inundator phase failed: {len(failed_jobs)} failures, {len(completed_outputs)}/{expected_outputs} successful outputs."
                )

            logging.info(
                f"[{self.pipeline_id}] All {len(completed_outputs)} tracked inundator jobs successful."
            )

            # --- Step 3: Dispatch Mosaicker ---
            self.state = PipelineState.DISPATCHING_MOSAICKER
            logging.info(
                f"[{self.pipeline_id}] Dispatching mosaicker job with {len(completed_outputs)} inputs."
            )
            mosaicker_meta = pipeline_params.prepare_mosaicker_dispatch_meta(
                self.config,
                self.pipeline_id,
                completed_outputs,
            )
            mosaicker_job_name = self.config.jobs.fim_mosaicker
            mosaicker_instance_prefix = f"mosaic-{self.pipeline_id[:8]}"
            try:
                mosaicker_dispatch_result = await self.nomad_client.dispatch_job(
                    mosaicker_job_name, mosaicker_instance_prefix, mosaicker_meta
                )
                mosaicker_dispatched_id = mosaicker_dispatch_result.get(
                    "DispatchedJobID"
                )
                if not mosaicker_dispatched_id:
                    logging.error(
                        f"[{self.pipeline_id}] Mosaicker dispatch response missing DispatchedJobID: {mosaicker_dispatch_result}"
                    )
                    raise ValueError(
                        "Mosaicker dispatch response missing DispatchedJobID."
                    )
                logging.info(
                    f"[{self.pipeline_id}] Mosaicker job dispatch succeeded: {mosaicker_dispatched_id}. Initiating tracking..."
                )
                mosaicker_future = await self.job_monitor.track_job(
                    mosaicker_dispatched_id, mosaicker_meta
                )
            except Exception as dispatch_e:
                logging.exception(
                    f"[{self.pipeline_id}] Mosaicker dispatch or tracking initiation failed."
                )
                raise RuntimeError("Mosaicker dispatch/tracking failed") from dispatch_e
            logging.info(
                f"[{self.pipeline_id}] Mosaicker job {mosaicker_dispatched_id} tracking initiated. Awaiting completion..."
            )

            # --- Step 4: Await Mosaicker ---
            self.state = PipelineState.AWAITING_MOSAICKER
            try:
                mosaicker_result = await mosaicker_future
                if (
                    not mosaicker_result
                    or not isinstance(mosaicker_result, str)
                    or not mosaicker_result.startswith("s3://")
                ):
                    logging.error(
                        f"[{self.pipeline_id}] Mosaicker job {mosaicker_dispatched_id} finished with unexpected result: {mosaicker_result}"
                    )
                    raise ValueError(
                        f"Mosaicker job {mosaicker_dispatched_id} returned unexpected result"
                    )
            except Exception as await_e:
                logging.exception(
                    f"[{self.pipeline_id}] Mosaicker job {mosaicker_dispatched_id} failed during await/execution."
                )
                raise await_e

            logging.info(
                f"[{self.pipeline_id}] Mosaicker job {mosaicker_dispatched_id} successful. Final Output: {mosaicker_result}"
            )
            self._final_result = mosaicker_result
            self.state = PipelineState.COMPLETED
            logging.info(f"[{self.pipeline_id}] Pipeline COMPLETED successfully.")

        except BaseException as e:  # Catch Exception and CancelledError
            if isinstance(e, asyncio.CancelledError):
                logging.warning(
                    f"[{self.pipeline_id}] Pipeline execution was cancelled."
                )
                self.state = PipelineState.FAILED  # Or a specific CANCELLED state
                self._failure_reason = e
                raise
            else:
                logging.exception(
                    f"[{self.pipeline_id}] Pipeline execution failed at state {self.state}"
                )
                self.state = PipelineState.FAILED
                self._failure_reason = e
                raise  # Re-raise so coordinator task sees failure

    async def cleanup(self):
        """Cleans up temporary directory for this pipeline."""
        logging.info(f"[{self.pipeline_id}] Cleaning up pipeline resources...")
        if self.temp_dir and os.path.isdir(self.temp_dir):
            with suppress(Exception):
                import shutil

                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, shutil.rmtree, self.temp_dir)
                logging.debug(
                    f"[{self.pipeline_id}] Removed temp directory: {self.temp_dir}"
                )
        else:
            logging.debug(
                f"[{self.pipeline_id}] Temp directory not found/set or already removed: {self.temp_dir}"
            )
        logging.info(f"[{self.pipeline_id}] Cleanup finished.")

    def get_result(self) -> Any:
        return self._final_result

    def get_failure_reason(self) -> Optional[BaseException]:
        return self._failure_reason
