import asyncio
import logging
from typing import Dict, List, Optional, Any
from contextlib import asynccontextmanager, suppress
import uuid
import aiohttp

from load_config import AppConfig
from nomad_api import NomadApiClient
from job_monitor import NomadJobMonitor
from data_service import DataService
from pipeline import PolygonPipeline, PipelineState


class PipelineCoordinator:
    """Manages the execution and lifecycle of multiple PolygonPipelines."""

    def __init__(self, config: AppConfig, http_session: aiohttp.ClientSession):
        self.config = config
        # Pass http_session to NomadApiClient
        self.nomad_client = NomadApiClient(config, http_session)
        self.job_monitor = NomadJobMonitor(self.nomad_client)
        self.data_service = DataService(config)
        self._active_pipeline_tasks: Dict[str, asyncio.Task] = {}
        self._stop_requested = False
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def _pipeline_context(self, pipeline: PolygonPipeline):
        initialized_ok = False
        try:
            initialized_ok = await pipeline.initialize()
            if not initialized_ok:
                raise ValueError(
                    f"Pipeline initialization failed", pipeline.get_failure_reason()
                )
            logging.info(f"Context: Entered for pipeline {pipeline.pipeline_id}")
            yield pipeline
        except Exception as e:
            logging.exception(
                f"Context: Pipeline {pipeline.pipeline_id} exited context with error: {e}"
            )
            pipeline.state = PipelineState.FAILED
            if pipeline._failure_reason is None:
                pipeline._failure_reason = e
                raise
        finally:
            logging.info(
                f"Context: Exiting for pipeline {pipeline.pipeline_id}, final state: {pipeline.state}"
            )
            await pipeline.cleanup()
            async with self._lock:
                if pipeline.pipeline_id in self._active_pipeline_tasks:
                    del self._active_pipeline_tasks[pipeline.pipeline_id]

    async def _run_single_pipeline_task(
        self, polygon_data: Dict, pipeline_id: str
    ) -> Dict:
        pipeline: Optional[PolygonPipeline] = None
        task_status = PipelineState.FAILED
        task_result = None
        task_error: Optional[BaseException] = None
        try:
            pipeline = PolygonPipeline(
                pipeline_id,
                polygon_data,
                self.config,
                self.nomad_client,
                self.data_service,
                self.job_monitor,
            )
            async with self._pipeline_context(pipeline):
                await pipeline.run()
            task_status = pipeline.state
            task_result = pipeline.get_result()
            task_error = pipeline.get_failure_reason()
        except ValueError as init_err:
            logging.error(
                f"Pipeline {pipeline_id} failed during init/context: {init_err}"
            )
            task_status = PipelineState.FAILED
            task_error = init_err.args[1] if len(init_err.args) > 1 else init_err
        except asyncio.CancelledError:
            logging.warning(f"Pipeline task for {pipeline_id} cancelled.")
            task_status = PipelineState.FAILED
            task_error = asyncio.CancelledError("Pipeline task cancelled")
        except Exception as e:
            logging.exception(
                f"Unexpected error running pipeline task for {pipeline_id}"
            )
            task_status = PipelineState.FAILED
            task_error = e
        # Ensure cleanup runs if exception happens before context manager's finally (shouldn't normally happen)
        finally:
            if pipeline and not getattr(
                pipeline, "_is_cleaned_up", True
            ):  # Check if cleanup was missed
                logging.warning(
                    f"Pipeline {pipeline_id} task exiting, ensuring cleanup runs."
                )
                await pipeline.cleanup()
        return {
            "pipeline_id": pipeline_id,
            "status": task_status,
            "result": task_result,
            "error": task_error,
        }

    async def run_multipolygon_pipeline(
        self, multipolygon_data: List[Dict]
    ) -> List[Dict]:
        if self._stop_requested:
            logging.warning("Coordinator stopped.")
            return []
        await self.job_monitor.start()

        async with self._lock:
            pipeline_tasks = []
            task_to_pipeline_id = {}
            for poly_data in multipolygon_data:
                if self._stop_requested:
                    break
                pipeline_id = str(uuid.uuid4())
                pipeline_task = asyncio.create_task(
                    self._run_single_pipeline_task(poly_data, pipeline_id),
                    name=f"PipelineTask-{pipeline_id}",
                )
                self._active_pipeline_tasks[pipeline_id] = pipeline_task
                task_to_pipeline_id[pipeline_task] = pipeline_id
                pipeline_tasks.append(pipeline_task)
        results = await asyncio.gather(*pipeline_tasks, return_exceptions=True)
        async with self._lock:
            for task in pipeline_tasks:
                p_id = task_to_pipeline_id.get(task)
                if p_id and p_id in self._active_pipeline_tasks:
                    del self._active_pipeline_tasks[p_id]
        final_results = []
        for i, res in enumerate(results):
            original_task = pipeline_tasks[i]
            p_id = task_to_pipeline_id.get(original_task, "N/A")
            if isinstance(res, BaseException):
                logging.error(
                    f"Pipeline task {original_task.get_name()} failed: {res}",
                    exc_info=False,
                )
                final_results.append(
                    {
                        "pipeline_id": p_id,
                        "status": PipelineState.FAILED,
                        "error": res,
                    }
                )
            elif isinstance(res, dict):
                final_results.append(res)
            else:
                logging.error(
                    f"Unexpected result type from task {original_task.get_name()}: {type(res)}"
                )
                final_results.append(
                    {
                        "pipeline_id": p_id,
                        "status": PipelineState.FAILED,
                        "error": RuntimeError("Unknown task result type"),
                    }
                )
        return final_results

    async def shutdown(self):
        if self._stop_requested:
            logging.info("Coordinator shutdown requested.")
            self._stop_requested = True
            return
        async with self._lock:
            active_tasks = list(self._active_pipeline_tasks.values())
            if active_tasks:
                logging.info(f"Cancelling {len(active_tasks)} active pipeline tasks...")
                for task in active_tasks:
                    task.cancel()

                logging.info("Active pipeline tasks cancelled.")
                await asyncio.gather(*active_tasks, return_exceptions=True)
            self._active_pipeline_tasks.clear()
        await self.job_monitor.stop()
        # Session closed externally
        logging.info("Coordinator shutdown complete (external sessions remain open).")
