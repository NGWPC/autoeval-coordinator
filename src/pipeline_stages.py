import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List

from pydantic import BaseModel, field_serializer

from load_config import AppConfig
from pipeline_utils import PathFactory, PipelineResult

logger = logging.getLogger(__name__)


class DispatchMetaBase(BaseModel):
    """
    Common parameters for all dispatched jobs.
    """

    fim_type: str
    registry_token: str
    aws_access_key: str
    aws_secret_key: str
    aws_session_token: str


class InundationDispatchMeta(DispatchMetaBase):
    """
    Metadata for the HAND inundator job.
    """

    catchment_data_path: str
    forecast_path: str
    output_path: str


class MosaicDispatchMeta(DispatchMetaBase):
    raster_paths: List[str]
    output_path: str
    clip_geometry_path: str = ""

    @field_serializer("raster_paths", mode="plain")
    def _ser_raster(self, v: List[str], info):
        # mosaic.py expects space-separated paths
        return " ".join(v)


class AgreementDispatchMeta(DispatchMetaBase):
    """
    Metadata for the agreement_maker job.
    """

    candidate_path: str
    benchmark_path: str
    output_path: str
    metrics_path: str = ""
    clip_geoms: str = ""


class PipelineStage(ABC):
    def __init__(
        self,
        config: AppConfig,
        nomad_service,
        data_service,
        path_factory: PathFactory,
        tags: Dict[str, str],
    ):
        self.config = config
        self.nomad = nomad_service
        self.data_svc = data_service
        self.path_factory = path_factory
        self.tags = tags

    @abstractmethod
    async def run(self, results: List[PipelineResult]) -> List[PipelineResult]:
        """Run this stage and return updated results."""
        pass

    @abstractmethod
    def filter_inputs(self, results: List[PipelineResult]) -> List[PipelineResult]:
        """Filter which results can be processed by this stage."""
        pass

    def log_stage_start(self, stage_name: str, input_count: int):
        logger.debug(f"{stage_name}: Starting with {input_count} inputs")

    def log_stage_complete(self, stage_name: str, success_count: int, total_count: int):
        logger.debug(f"{stage_name}: {success_count}/{total_count} succeeded")

    def _get_base_meta_kwargs(self) -> Dict[str, str]:
        return {
            "fim_type": self.config.defaults.fim_type,
            "registry_token": self.config.nomad.registry_token or "",
            "aws_access_key": self.config.aws.AWS_ACCESS_KEY_ID or "",
            "aws_secret_key": self.config.aws.AWS_SECRET_ACCESS_KEY or "",
            "aws_session_token": self.config.aws.AWS_SESSION_TOKEN or "",
        }

    def _create_tags_str(self, internal_tags: Dict[str, str]) -> str:
        """
        Create a tags string with internal tags added.
        Format: [key1=val1,key2=val2,...]
        """
        all_tags = {**self.tags, **internal_tags}

        # Order tags: batch_name, aoi_name first, then internal tags, then others
        ordered_keys = []
        if "batch_name" in all_tags:
            ordered_keys.append("batch_name")
        if "aoi_name" in all_tags:
            ordered_keys.append("aoi_name")

        # Add internal tags in a consistent order
        internal_order = ["bench_src", "cand_src", "scenario", "catchment"]
        for key in internal_order:
            if key in all_tags and key not in ordered_keys:
                ordered_keys.append(key)

        for key in sorted(all_tags.keys()):
            if key not in ordered_keys:
                ordered_keys.append(key)

        tag_str = ",".join(f"{k}={all_tags[k]}" for k in ordered_keys)
        return f"[{tag_str}]"


class InundationStage(PipelineStage):
    """Stage that runs inundation jobs for all catchments in all scenarios."""

    def __init__(
        self,
        config: AppConfig,
        nomad_service,
        data_service,
        path_factory: PathFactory,
        tags: Dict[str, str],
        catchments: Dict[str, Dict[str, Any]],
    ):
        super().__init__(config, nomad_service, data_service, path_factory, tags)
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
                output_path = self.path_factory.inundation_output_path(
                    result.collection_name, result.scenario_name, catch_id
                )
                result.set_path("inundation", f"catchment_{catch_id}", output_path)

                task = asyncio.create_task(self._process_catchment(result, catch_id, catchment_info, output_path))
                tasks.append(task)
                task_metadata.append((result, catch_id, output_path))

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

        # Validate files and update results
        updated_results = []
        for result in valid_results:
            outputs = scenario_outputs.get(result.scenario_id, [])
            if outputs:
                valid_outputs = await self.data_svc.validate_files(outputs)
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

        # Parquet files are stored in catchment-data-indices directory
        parquet_path = await self.data_svc.copy_file_to_uri(
            local_parquet,
            self.path_factory.catchment_parquet_path(catch_id),
        )
        flowfile_s3_path = await self.data_svc.copy_file_to_uri(
            result.flowfile_path, self.path_factory.flowfile_path(result.collection_name, result.scenario_name)
        )

        meta = self._create_inundation_meta(parquet_path, flowfile_s3_path, output_path)

        # add bench_src, scenario, and catchment internal tags
        internal_tags = {
            "bench_src": result.collection_name,
            "scenario": result.scenario_name,
            "catchment": str(catch_id)[:7]
        }
        tags_str = self._create_tags_str(internal_tags)

        job_id, _ = await self.nomad.dispatch_and_track(
            self.config.jobs.hand_inundator,
            prefix=tags_str,
            meta=meta.model_dump(),
        )
        logger.debug(f"[{result.scenario_id}/{catch_id}] inundator done → {job_id}")
        return job_id

    def _create_inundation_meta(
        self, catchment_path: str, forecast_path: str, output_path: str
    ) -> InundationDispatchMeta:
        return InundationDispatchMeta(
            catchment_data_path=catchment_path,
            forecast_path=forecast_path,
            output_path=output_path,
            **self._get_base_meta_kwargs(),
        )


class MosaicStage(PipelineStage):
    """Stage that creates HAND and benchmark mosaics from inundation outputs."""

    def __init__(
        self,
        config: AppConfig,
        nomad_service,
        data_service,
        path_factory: PathFactory,
        tags: Dict[str, str],
        aoi_path: str,
    ):
        super().__init__(config, nomad_service, data_service, path_factory, tags)
        self.aoi_path = aoi_path
        self._clip_geometry_s3_path = None

    def filter_inputs(self, results: List[PipelineResult]) -> List[PipelineResult]:
        """Filter results that have valid inundation outputs."""
        return [r for r in results if r.status == "inundation_complete" and r.get_path("inundation", "valid_outputs")]

    async def run(self, results: List[PipelineResult]) -> List[PipelineResult]:
        """Run mosaic jobs for scenarios with valid inundation outputs."""
        valid_results = self.filter_inputs(results)
        self.log_stage_start("Mosaic", len(valid_results))

        # Copy AOI file to S3 once for all mosaic jobs
        if valid_results and self.aoi_path:
            self._clip_geometry_s3_path = await self.data_svc.copy_file_to_uri(
                self.aoi_path,
                self.path_factory.aoi_path()
            )
            logger.debug(f"Copied AOI file to S3: {self._clip_geometry_s3_path}")

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
            hand_output_path = self.path_factory.hand_mosaic_path(result.collection_name, result.scenario_name)
            result.set_path("mosaic", "hand", hand_output_path)
            hand_meta = self._create_mosaic_meta(valid_outputs, hand_output_path)

            # add cand_src and scenario internal tags for HAND mosaic
            hand_internal_tags = {"cand_src": "hand", "scenario": result.scenario_name}
            hand_tags_str = self._create_tags_str(hand_internal_tags)

            hand_task = asyncio.create_task(
                self.nomad.dispatch_and_track(
                    self.config.jobs.fim_mosaicker,
                    prefix=hand_tags_str,
                    meta=hand_meta.model_dump(),
                )
            )
            hand_tasks.append(hand_task)

            # Benchmark mosaic
            benchmark_output_path = self.path_factory.benchmark_mosaic_path(
                result.collection_name, result.scenario_name
            )
            result.set_path("mosaic", "benchmark", benchmark_output_path)
            benchmark_meta = self._create_mosaic_meta(benchmark_rasters, benchmark_output_path)

            # add bench_src and scenario internal tags for benchmark mosaic
            benchmark_internal_tags = {"bench_src": result.collection_name, "scenario": result.scenario_name}
            benchmark_tags_str = self._create_tags_str(benchmark_internal_tags)

            benchmark_task = asyncio.create_task(
                self.nomad.dispatch_and_track(
                    self.config.jobs.fim_mosaicker,
                    prefix=benchmark_tags_str,
                    meta=benchmark_meta.model_dump(),
                )
            )
            benchmark_tasks.append(benchmark_task)
            task_results.append(result)

        if not hand_tasks:
            self.log_stage_complete("Mosaic", 0, len(valid_results))
            return []

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
        return MosaicDispatchMeta(
            raster_paths=raster_paths,
            output_path=output_path,
            clip_geometry_path=self._clip_geometry_s3_path or "",
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

            agreement_output_path = self.path_factory.agreement_map_path(result.collection_name, result.scenario_name)
            metrics_output_path = self.path_factory.agreement_metrics_path(result.collection_name, result.scenario_name)

            result.set_path("agreement", "map", agreement_output_path)
            result.set_path("agreement", "metrics", metrics_output_path)

            meta = self._create_agreement_meta(
                hand_mosaic, benchmark_mosaic, agreement_output_path, metrics_output_path
            )

            # Create tags string with bench_src and scenario internal tags for agreement
            agreement_internal_tags = {"bench_src": result.collection_name, "scenario": result.scenario_name}
            agreement_tags_str = self._create_tags_str(agreement_internal_tags)

            task = asyncio.create_task(
                self.nomad.dispatch_and_track(
                    self.config.jobs.agreement_maker,
                    prefix=agreement_tags_str,
                    meta=meta.model_dump(),
                )
            )
            tasks.append(task)
            task_results.append(result)

        if not tasks:
            self.log_stage_complete("Agreement", 0, len(valid_results))
            return []

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
        return AgreementDispatchMeta(
            candidate_path=candidate_path,
            benchmark_path=benchmark_path,
            output_path=output_path,
            metrics_path=metrics_path,
            **self._get_base_meta_kwargs(),
        )
