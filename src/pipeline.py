import asyncio
import json
import logging
import os
from enum import Enum
from typing import Dict, Optional, Any, List
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
        polygon_id: str,
        pipeline_id: str,
        polygon_data: Dict,
        config: AppConfig,
        nomad_client: NomadApiClient,
        data_service: DataService,
        job_monitor: NomadJobMonitor,
    ):
        self.polygon_id = polygon_id
        self.pipeline_id = pipeline_id
        self.instance_id = f"{pipeline_id}-{polygon_id}"
        self.polygon_data = polygon_data
        self.config = config
        self.nomad_client = nomad_client
        self.data_service = data_service
        self.job_monitor = job_monitor
        self.state = PipelineState.INITIALIZING
        self.catchment_data: Optional[Dict] = None
        self.hand_version: Optional[str] = None
        self.temp_dir: Optional[str] = None
        self._final_result: Any = None
        self._failure_reason: Optional[BaseException] = None
        logging.info(
            f"Pipeline {self.pipeline_id} created for polygon {self.polygon_id}"
        )

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

    async def run(self):
        """Executes the pipeline steps using smart_open for intermediate JSON."""
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
        inundator_futures: Dict[str, asyncio.Future] = {}
        dispatched_ids: Dict[str, str] = {}
        try:
            # --- Step 1: Prepare, Write Intermediate JSON, and Dispatch Inundators ---
            self.state = PipelineState.DISPATCHING_INUNDATORS
            logging.info(
                f"[{self.pipeline_id}] Preparing, writing JSONs, & Dispatching {len(self.catchment_data)} inundator jobs."
            )
            write_tasks = []
            dispatch_tasks = []
            future_tracking_map = {}
            prepared_params_meta = []  # Store (catchment_id, dispatch_meta)

            # Prepare parameters and create write/dispatch coroutines
            for catchment_id, c_info in self.catchment_data.items():
                try:
                    _, json_content, dispatch_meta = (
                        pipeline_params.prepare_inundator_dispatch_info(
                            self.config,
                            self.pipeline_id,
                            self.polygon_id,
                            self.polygon_data,
                            catchment_id,
                            c_info,
                            self.hand_version,
                        )
                    )
                    prepared_params_meta.append((catchment_id, dispatch_meta))

                    # Create coroutine to write JSON to S3 URI specified in meta
                    s3_json_uri = dispatch_meta["catchment_data_path"]
                    write_coro = self.data_service.write_json_to_uri(
                        json_content, s3_json_uri
                    )
                    write_tasks.append(write_coro)

                    # Create dispatch coroutine
                    job_name = self.config.jobs.hand_inundator
                    instance_prefix = f"inundate-{self.pipeline_id[:8]}-{catchment_id}"
                    dispatch_coro = self.nomad_client.dispatch_job(
                        job_name, instance_prefix, dispatch_meta
                    )
                    dispatch_tasks.append(dispatch_coro)
                    future_tracking_map[dispatch_coro] = (catchment_id, dispatch_meta)

                except Exception as prep_e:
                    raise RuntimeError(
                        f"Parameter preparation failed for {catchment_id}"
                    ) from prep_e

            # Run JSON writes and dispatches concurrently
            logging.info(
                f"[{self.pipeline_id}] Starting {len(write_tasks)} JSON writes and {len(dispatch_tasks)} dispatches..."
            )
            all_tasks = write_tasks + dispatch_tasks
            results = await asyncio.gather(*all_tasks, return_exceptions=True)

            # Process results - Check writes first (optional, could check interleaved)
            write_results = results[: len(write_tasks)]
            dispatch_results = results[len(write_tasks) :]

            for i, res in enumerate(write_results):
                if isinstance(res, Exception):
                    # Find corresponding input data for logging
                    catchment_id, _ = prepared_params_meta[i]  # Relies on order
                    logging.error(
                        f"[{self.pipeline_id}] Write JSON failed for {catchment_id}: {res}"
                    )
                    raise ConnectionError(
                        f"Write JSON failed for {catchment_id}"
                    ) from res

            logging.info(f"[{self.pipeline_id}] JSON writes complete.")

            # Process dispatch results and track futures
            successful_dispatches = 0
            for i, result in enumerate(dispatch_results):
                dispatch_coro = dispatch_tasks[i]
                catchment_id, original_meta = future_tracking_map[dispatch_coro]
                if isinstance(result, Exception):
                    logging.error(
                        f"[{self.pipeline_id}] Dispatch failed for catchment {catchment_id}."
                    )
                    raise result  # Re-raise original exception
                elif isinstance(result, dict) and result.get("DispatchedJobID"):
                    dispatched_job_id = result["DispatchedJobID"]
                    try:
                        future = await self.job_monitor.track_job(
                            dispatched_job_id, original_meta
                        )
                    except Exception as track_e:
                        raise RuntimeError(
                            f"Failed track job {dispatched_job_id}"
                        ) from track_e
                    inundator_futures[dispatched_job_id] = future
                    dispatched_ids[catchment_id] = dispatched_job_id
                    successful_dispatches += 1
                else:
                    raise RuntimeError(f"Unknown dispatch result for {catchment_id}")

            if successful_dispatches == 0:
                raise RuntimeError("No inundator jobs dispatched successfully.")
            logging.info(
                f"[{self.pipeline_id}] Successfully dispatched/tracking {successful_dispatches} jobs. Awaiting..."
            )

            # --- Step 2: Await Inundators ---
            self.state = PipelineState.AWAITING_INUNDATORS
            inundator_results = await asyncio.gather(
                *inundator_futures.values(), return_exceptions=True
            )
            completed_outputs: List[str] = []
            failed_jobs: List[str] = []
            first_job_failure: Optional[BaseException] = None
            for i, result in enumerate(inundator_results):
                dispatched_job_id = list(inundator_futures.keys())[i]
                catchment_id = next(
                    (
                        cid
                        for cid, d_id in dispatched_ids.items()
                        if d_id == dispatched_job_id
                    ),
                    None,
                )
                if isinstance(result, BaseException):
                    logging.error(
                        f"[{self.pipeline_id}] Inundator job {dispatched_job_id} ({catchment_id}) failed: {result}"
                    )
                    failed_jobs.append(dispatched_job_id)
                    if first_job_failure is None:
                        first_job_failure = result
                elif result:
                    logging.info(
                        f"[{self.pipeline_id}] Inundator job {dispatched_job_id} ({catchment_id}) succeeded: {result}"
                    )
                    completed_outputs.append(str(result))
                else:
                    logging.error(
                        f"[{self.pipeline_id}] Inundator job {dispatched_job_id} finished with unexpected result: {result}"
                    )
                    failed_jobs.append(dispatched_job_id)
                    if first_job_failure is None:
                        first_job_failure = ValueError(
                            f"Unexpected result {dispatched_job_id}"
                        )
            if failed_jobs or not completed_outputs:
                raise first_job_failure or RuntimeError(
                    f"{len(failed_jobs)} inundator jobs failed or no output."
                )
            logging.info(
                f"[{self.pipeline_id}] All {len(completed_outputs)} tracked inundator jobs successful."
            )

            # --- Step 3: Dispatch Mosaicker ---
            self.state = PipelineState.DISPATCHING_MOSAICKER
            # (Dispatch logic remains the same - uses pipeline_parameters)
            logging.info(f"[{self.pipeline_id}] Dispatching mosaicker job.")
            mosaicker_meta = pipeline_params.prepare_mosaicker_dispatch_meta(
                self.config,
                self.pipeline_id,
                self.polygon_id,
                self.polygon_data,
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
                    raise ValueError(
                        "Mosaicker dispatch response missing DispatchedJobID."
                    )
                mosaicker_future = await self.job_monitor.track_job(
                    mosaicker_dispatched_id, mosaicker_meta
                )
            except Exception as dispatch_e:
                raise RuntimeError("Mosaicker dispatch/tracking failed") from dispatch_e
            logging.info(
                f"[{self.pipeline_id}] Mosaicker job {mosaicker_dispatched_id} dispatched. Awaiting..."
            )

            # --- Step 4: Await Mosaicker ---
            self.state = PipelineState.AWAITING_MOSAICKER
            # (Await logic remains the same)
            try:
                mosaicker_result = await mosaicker_future
            except Exception as await_e:
                logging.error(
                    f"[{self.pipeline_id}] Mosaicker job {mosaicker_dispatched_id} failed during await."
                )
                raise await_e

            logging.info(
                f"[{self.pipeline_id}] Mosaicker job {mosaicker_dispatched_id} successful: {mosaicker_result}"
            )
            self._final_result = mosaicker_result
            self.state = PipelineState.COMPLETED
            logging.info(f"[{self.pipeline_id}] Pipeline COMPLETED successfully.")

        except BaseException as e:  # Catch Exception and CancelledError
            logging.exception(f"[{self.pipeline_id}] Pipeline execution failed")
            self.state = PipelineState.FAILED
            self._failure_reason = e
            raise  # Re-raise so coordinator task sees failure

    async def cleanup(self):
        """Cleans up temporary directory for this pipeline."""
        # (Implementation remains the same - removes temp dir)
        logging.info(f"[{self.pipeline_id}] Cleaning up pipeline resources...")
        if self.temp_dir and os.path.isdir(self.temp_dir):
            with suppress(Exception):
                import shutil

                shutil.rmtree(self.temp_dir)
                logging.debug(
                    f"[{self.pipeline_id}] Removed temp directory: {self.temp_dir}"
                )
        else:
            logging.debug(
                f"[{self.pipeline_id}] Temp directory not found/set: {self.temp_dir}"
            )
        logging.info(f"[{self.pipeline_id}] Cleanup finished.")

    def get_result(self) -> Any:
        return self._final_result

    def get_failure_reason(self) -> Optional[BaseException]:
        return self._failure_reason
