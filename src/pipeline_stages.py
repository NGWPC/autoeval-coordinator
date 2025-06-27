import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List

from load_config import AppConfig
from nomad_service import (
    AgreementDispatchMeta,
    InundationDispatchMeta,
    MosaicDispatchMeta,
)
from pipeline_utils import PathFactory, PipelineResult

logger = logging.getLogger(__name__)


class PipelineStage(ABC):
    """Abstract base class for pipeline stages."""

    def __init__(self, config: AppConfig, nomad_service, data_service, path_factory: PathFactory, pipeline_id: str):
        self.config = config
        self.nomad = nomad_service
        self.data_svc = data_service
        self.path_factory = path_factory
        self.pipeline_id = pipeline_id

    @abstractmethod
    async def run(self, results: List[PipelineResult]) -> List[PipelineResult]:
        """Run this stage and return updated results."""
        pass

    @abstractmethod
    def filter_inputs(self, results: List[PipelineResult]) -> List[PipelineResult]:
        """Filter which results can be processed by this stage."""
        pass

    def log_stage_start(self, stage_name: str, input_count: int):
        """Log the start of a stage."""
        logger.debug(f"[{self.pipeline_id}] {stage_name}: Starting with {input_count} inputs")

    def log_stage_complete(self, stage_name: str, success_count: int, total_count: int):
        """Log the completion of a stage."""
        logger.debug(f"[{self.pipeline_id}] {stage_name}: {success_count}/{total_count} succeeded")

    def _get_base_meta_kwargs(self) -> Dict[str, str]:
        """Get common metadata kwargs for all job types."""
        return {
            "fim_type": self.config.defaults.fim_type,
            "registry_token": self.config.nomad.registry_token or "",
            "aws_access_key": self.config.s3.AWS_ACCESS_KEY_ID or "",
            "aws_secret_key": self.config.s3.AWS_SECRET_ACCESS_KEY or "",
            "aws_session_token": self.config.s3.AWS_SESSION_TOKEN or "",
        }


class InundationStage(PipelineStage):
    """Stage that runs inundation jobs for all catchments in all scenarios."""

    def __init__(
        self,
        config: AppConfig,
        nomad_service,
        data_service,
        path_factory: PathFactory,
        pipeline_id: str,
        catchments: Dict[str, Dict[str, Any]],
    ):
        super().__init__(config, nomad_service, data_service, path_factory, pipeline_id)
        self.catchments = catchments

    def filter_inputs(self, results: List[PipelineResult]) -> List[PipelineResult]:
        """All results are valid for inundation stage."""
        return results

    async def run(self, results: List[PipelineResult]) -> List[PipelineResult]:
        """Run inundation jobs for all catchments in all scenarios."""
        valid_results = self.filter_inputs(results)
        self.log_stage_start("Inundation", len(valid_results))

        # Create tasks for all catchment/scenario combinations
        tasks = []
        task_metadata = []

        for result in valid_results:
            for catch_id, catchment_info in self.catchments.items():
                output_path = self.path_factory.inundation_output_path(result.scenario_id, catch_id)
                result.set_path("inundation", f"catchment_{catch_id}", output_path)

                task = asyncio.create_task(self._process_catchment(result, catch_id, catchment_info, output_path))
                tasks.append(task)
                task_metadata.append((result, catch_id, output_path))

        # Wait for all tasks
        task_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Group results by scenario and validate outputs
        scenario_outputs = {}
        for (result, catch_id, output_path), task_result in zip(task_metadata, task_results):
            if result.scenario_id not in scenario_outputs:
                scenario_outputs[result.scenario_id] = []

            if not isinstance(task_result, Exception):
                scenario_outputs[result.scenario_id].append(output_path)
            else:
                logger.error(f"[{result.scenario_id}] Catchment {catch_id} inundation failed: {task_result}")

        # Validate S3 files and update results
        updated_results = []
        for result in valid_results:
            outputs = scenario_outputs.get(result.scenario_id, [])
            if outputs:
                valid_outputs = await self.data_svc.validate_s3_files(outputs)
                if valid_outputs:
                    result.set_path("inundation", "valid_outputs", valid_outputs)
                    result.status = "inundation_complete"
                    updated_results.append(result)
                    logger.debug(f"[{result.scenario_id}] {len(valid_outputs)}/{len(outputs)} inundation outputs valid")
                else:
                    result.mark_failed("No valid inundation outputs")
            else:
                result.mark_failed("No inundation outputs produced")

        self.log_stage_complete("Inundation", len(updated_results), len(valid_results))
        return updated_results

    async def _process_catchment(
        self, result: PipelineResult, catch_id: str, catchment_info: Dict[str, Any], output_path: str
    ) -> str:
        """Process a single catchment for a scenario."""
        # Copy files to S3
        local_parquet = catchment_info.get("parquet_path")
        if not local_parquet:
            raise ValueError(f"No parquet_path found for catchment {catch_id}")

        parquet_path = await self.data_svc.copy_file_to_uri(
            local_parquet, self.path_factory.catchment_path(result.scenario_id, catch_id, "catchment_data.parquet")
        )
        flowfile_s3_path = await self.data_svc.copy_file_to_uri(
            result.flowfile_path, self.path_factory.catchment_path(result.scenario_id, catch_id, "flowfile.csv")
        )

        # Create job metadata and run
        meta = self._create_inundation_meta(parquet_path, flowfile_s3_path, output_path)

        job_id = await self.nomad.run_job(
            self.config.jobs.hand_inundator,
            instance_prefix=f"inund-{result.scenario_id}-catch-{str(catch_id)}---",
            meta=meta.model_dump(),
            pipeline_id=self.pipeline_id,
        )
        logger.debug(f"[{result.scenario_id}/{catch_id}] inundator done → {job_id}")
        return job_id

    def _create_inundation_meta(
        self, catchment_path: str, forecast_path: str, output_path: str
    ) -> InundationDispatchMeta:
        """Create inundation job metadata."""
        return InundationDispatchMeta(
            catchment_data_path=catchment_path,
            forecast_path=forecast_path,
            output_path=output_path,
            **self._get_base_meta_kwargs(),
        )


class MosaicStage(PipelineStage):
    """Stage that creates HAND and benchmark mosaics from inundation outputs."""

    def filter_inputs(self, results: List[PipelineResult]) -> List[PipelineResult]:
        """Filter results that have valid inundation outputs."""
        return [r for r in results if r.status == "inundation_complete" and r.get_path("inundation", "valid_outputs")]

    async def run(self, results: List[PipelineResult]) -> List[PipelineResult]:
        """Run mosaic jobs for scenarios with valid inundation outputs."""
        valid_results = self.filter_inputs(results)
        self.log_stage_start("Mosaic", len(valid_results))

        hand_tasks = []
        benchmark_tasks = []
        task_results = []

        for result in valid_results:
            valid_outputs = result.get_path("inundation", "valid_outputs")
            benchmark_rasters = result.benchmark_rasters

            if not valid_outputs or not benchmark_rasters:
                logger.warning(f"[{result.scenario_id}] Skipping mosaic - missing inputs")
                continue

            # HAND mosaic
            hand_output_path = self.path_factory.hand_mosaic_path(result.scenario_id)
            result.set_path("mosaic", "hand", hand_output_path)
            hand_meta = self._create_mosaic_meta(valid_outputs, hand_output_path)
            hand_task = asyncio.create_task(
                self.nomad.run_job(
                    self.config.jobs.fim_mosaicker,
                    instance_prefix=f"hand-mosaic-{result.scenario_id}",
                    meta=hand_meta.model_dump(),
                    pipeline_id=self.pipeline_id,
                )
            )
            hand_tasks.append(hand_task)

            # Benchmark mosaic
            benchmark_output_path = self.path_factory.benchmark_mosaic_path(result.scenario_id)
            result.set_path("mosaic", "benchmark", benchmark_output_path)
            benchmark_meta = self._create_mosaic_meta(benchmark_rasters, benchmark_output_path)
            benchmark_task = asyncio.create_task(
                self.nomad.run_job(
                    self.config.jobs.fim_mosaicker,
                    instance_prefix=f"bench-mosaic-{result.scenario_id}",
                    meta=benchmark_meta.model_dump(),
                    pipeline_id=self.pipeline_id,
                )
            )
            benchmark_tasks.append(benchmark_task)
            task_results.append(result)

        if not hand_tasks:
            self.log_stage_complete("Mosaic", 0, len(valid_results))
            return []

        # Wait for all mosaic tasks
        hand_results = await asyncio.gather(*hand_tasks, return_exceptions=True)
        benchmark_results = await asyncio.gather(*benchmark_tasks, return_exceptions=True)

        # Update results based on success/failure
        successful_results = []
        for result, hand_result, benchmark_result in zip(task_results, hand_results, benchmark_results):
            hand_failed = isinstance(hand_result, Exception)
            benchmark_failed = isinstance(benchmark_result, Exception)

            if hand_failed:
                logger.error(f"[{result.scenario_id}] HAND mosaic failed: {hand_result}")
            if benchmark_failed:
                logger.error(f"[{result.scenario_id}] Benchmark mosaic failed: {benchmark_result}")

            if not hand_failed and not benchmark_failed:
                result.status = "mosaic_complete"
                successful_results.append(result)
                logger.debug(f"[{result.scenario_id}] Mosaic stage complete")
            else:
                result.mark_failed("One or both mosaics failed")

        self.log_stage_complete("Mosaic", len(successful_results), len(valid_results))
        return successful_results

    def _create_mosaic_meta(self, raster_paths: List[str], output_path: str) -> MosaicDispatchMeta:
        """Create mosaic job metadata."""
        return MosaicDispatchMeta(
            raster_paths=raster_paths,
            output_path=output_path,
            **self._get_base_meta_kwargs(),
        )


class AgreementStage(PipelineStage):
    """Stage that creates agreement maps from HAND and benchmark mosaics."""

    def filter_inputs(self, results: List[PipelineResult]) -> List[PipelineResult]:
        """Filter results that have valid mosaic outputs."""
        return [r for r in results if r.status == "mosaic_complete"]

    async def run(self, results: List[PipelineResult]) -> List[PipelineResult]:
        """Run agreement jobs for scenarios with valid mosaic outputs."""
        valid_results = self.filter_inputs(results)
        self.log_stage_start("Agreement", len(valid_results))

        tasks = []
        task_results = []

        for result in valid_results:
            hand_mosaic = result.get_path("mosaic", "hand")
            benchmark_mosaic = result.get_path("mosaic", "benchmark")

            agreement_output_path = self.path_factory.agreement_map_path(result.scenario_id)
            metrics_output_path = self.path_factory.agreement_metrics_path(result.scenario_id)

            result.set_path("agreement", "map", agreement_output_path)
            result.set_path("agreement", "metrics", metrics_output_path)

            meta = self._create_agreement_meta(
                hand_mosaic, benchmark_mosaic, agreement_output_path, metrics_output_path
            )

            task = asyncio.create_task(
                self.nomad.run_job(
                    self.config.jobs.agreement_maker,
                    instance_prefix=f"agree-{result.scenario_id}",
                    meta=meta.model_dump(),
                    pipeline_id=self.pipeline_id,
                )
            )
            tasks.append(task)
            task_results.append(result)

        if not tasks:
            self.log_stage_complete("Agreement", 0, len(valid_results))
            return []

        # Wait for all agreement tasks
        task_job_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Update results based on success/failure
        successful_results = []
        for result, job_result in zip(task_results, task_job_results):
            if isinstance(job_result, Exception):
                result.mark_failed(f"Agreement job failed: {job_result}")
                logger.error(f"[{result.scenario_id}] Agreement job failed: {job_result}")
            else:
                result.mark_completed()
                successful_results.append(result)
                logger.debug(f"[{result.scenario_id}] Pipeline complete")

        self.log_stage_complete("Agreement", len(successful_results), len(valid_results))
        return successful_results

    def _create_agreement_meta(
        self, candidate_path: str, benchmark_path: str, output_path: str, metrics_path: str = ""
    ) -> AgreementDispatchMeta:
        """Create agreement job metadata."""
        return AgreementDispatchMeta(
            candidate_path=candidate_path,
            benchmark_path=benchmark_path,
            output_path=output_path,
            metrics_path=metrics_path,
            **self._get_base_meta_kwargs(),
        )