import argparse
import asyncio
import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from uuid import uuid4

import aiohttp
import geopandas as gpd

from data_service import DataService
from job_monitor import NomadJobMonitor
from load_config import AppConfig, load_config
from nomad_api import NomadApiClient
from nomad_service import (
    AgreementDispatchMeta,
    InundationDispatchMeta,
    MosaicDispatchMeta,
    NomadService,
)
from pipeline_log_db import PipelineLogDB
from pipeline_stages import PipelineStage
from pipeline_utils import PathFactory, PipelineResult

logger = logging.getLogger(__name__)


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


class PolygonPipeline:
    """
    Pipeline that processes multiple flow scenarios with stage-based parallelism:
    1) Initialize: Query STAC for scenarios and hand index for catchments
    2) Stage 1: Run all inundation jobs concurrently across all scenarios
    3) Stage 2: Run all mosaic jobs concurrently for scenarios with valid outputs
    4) Stage 3: Run all agreement jobs concurrently for scenarios with valid mosaics
    """

    def __init__(
        self,
        config: AppConfig,
        nomad: NomadService,
        data_svc: DataService,
        polygon_gdf: gpd.GeoDataFrame,
        pipeline_id: str,
        log_db: Optional[PipelineLogDB] = None,
    ):
        self.config = config
        self.nomad = nomad
        self.data_svc = data_svc
        self.polygon_gdf = polygon_gdf
        self.pipeline_id = pipeline_id  # This is the HUC code
        self.log_db = log_db
        self.tmp = tempfile.TemporaryDirectory(prefix=f"{pipeline_id}-")

        # Create path factory
        self.path_factory = PathFactory(config, pipeline_id)

        # Populated by initialize()
        self.catchments: Dict[str, Dict[str, Any]] = {}
        self.flow_scenarios: Dict[str, Dict[str, str]] = {}
        self.benchmark_scenarios: Dict[str, Dict[str, List[str]]] = {}

    async def initialize(self) -> None:
        """Query for catchments and flow scenarios."""
        # Clean up any stale jobs from previous runs if db_manager is available
        if self.log_db:
            await self.log_db.cleanup_pipeline_jobs(self.pipeline_id)
        # Query STAC for flow scenarios
        if (self.config.stac and self.config.stac.enabled) or self.config.mock_data_paths.mock_stac_results:
            logger.debug(f"[{self.pipeline_id}] Querying STAC for flow scenarios")
            stac_data = await self.data_svc.query_stac_for_flow_scenarios(self.polygon_gdf)
            self.flow_scenarios = stac_data.get("combined_flowfiles", {})

            # Extract benchmark rasters from STAC scenarios
            raw_scenarios = stac_data.get("scenarios", {})
            self.benchmark_scenarios = {}
            for collection, scenarios in raw_scenarios.items():
                self.benchmark_scenarios[collection] = {}
                for scenario_name, scenario_data in scenarios.items():
                    # Find extent key (could be extent_raster, extent, etc.)
                    extent_key = None
                    for key in scenario_data.keys():
                        if "extent" in key.lower():
                            extent_key = key
                            break
                    self.benchmark_scenarios[collection][scenario_name] = (
                        scenario_data.get(extent_key, []) if extent_key else []
                    )

            if self.flow_scenarios:
                logger.debug(
                    f"[{self.pipeline_id}] Found {len(self.flow_scenarios)} collections"
                    + (" (from mock data)" if stac_data.get("mock_data") else "")
                )

        if not self.flow_scenarios:
            raise RuntimeError(f"[{self.pipeline_id}] No flow scenarios found")

        # Query hand index for catchments
        logger.debug(f"[{self.pipeline_id}] Querying hand index for catchments")
        data = await self.data_svc.query_for_catchments(self.polygon_gdf)
        self.catchments = data.get("catchments", {})

        if not self.catchments:
            raise RuntimeError(f"[{self.pipeline_id}] No catchments found")

        total_scenarios = sum(len(scenarios) for scenarios in self.flow_scenarios.values())
        logger.info(
            f"[{self.pipeline_id}] Initialization complete: {len(self.catchments)} catchments, "
            f"{total_scenarios} flow scenarios"
        )

    async def run(self) -> Dict[str, Any]:
        """Run the pipeline with stage-based parallelism."""
        await self.initialize()

        # Build scenario results
        results = []
        for collection, flows in self.flow_scenarios.items():
            for scenario, flowfile_path in flows.items():
                benchmark_rasters = self.benchmark_scenarios.get(collection, {}).get(scenario, [])
                result = PipelineResult(
                    scenario_id=f"{self.pipeline_id}-{collection}-{scenario}",
                    collection_name=collection,
                    scenario_name=scenario,
                    flowfile_path=flowfile_path,
                    benchmark_rasters=benchmark_rasters,
                )
                results.append(result)

        logger.debug(f"[{self.pipeline_id}] Processing {len(results)} scenarios with stage-based parallelism")

        try:
            # Create stages
            inundation_stage = InundationStage(
                self.config, self.nomad, self.data_svc, self.path_factory, self.pipeline_id, self.catchments
            )
            mosaic_stage = MosaicStage(self.config, self.nomad, self.data_svc, self.path_factory, self.pipeline_id)
            agreement_stage = AgreementStage(
                self.config, self.nomad, self.data_svc, self.path_factory, self.pipeline_id
            )

            # Run stages sequentially
            results = await inundation_stage.run(results)
            results = await mosaic_stage.run(results)
            results = await agreement_stage.run(results)

            # Count successful scenarios
            successful_results = [r for r in results if r.status == "completed"]
            total_attempted = len([r for r in results if r.status != "pending"])

            result = {
                "status": "success",
                "pipeline_id": self.pipeline_id,
                "catchment_count": len(self.catchments),
                "total_scenarios_attempted": total_attempted,
                "successful_scenarios": len(successful_results),
                "message": f"Pipeline completed successfully with {len(successful_results)}/{total_attempted} scenarios",
            }
            logger.info(
                f"[{self.pipeline_id}] Pipeline SUCCESS: {len(successful_results)}/{total_attempted} scenarios completed"
            )
            return result
        except Exception as e:
            logger.error(f"[{self.pipeline_id}] Pipeline FAILED: {str(e)}")
            return {
                "status": "failed",
                "pipeline_id": self.pipeline_id,
                "error": str(e),
                "message": f"Pipeline failed: {str(e)}",
            }

    async def cleanup(self) -> None:
        """Clean up temporary resources."""
        self.tmp.cleanup()
        logger.debug(f"[{self.pipeline_id}] cleaned up temp files")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run one PolygonPipeline in isolation")
    parser.add_argument("--index", type=int, default=0, help="Which HUC index in the list to process")
    parser.add_argument(
        "--use-mock-polygon", action="store_true", help="Use polygon from mock data file instead of WBD"
    )
    parser.add_argument(
        "--config", default=os.path.join("config", "pipeline_config.yml"), help="Path to your YAML config"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    cfg = load_config(args.config)

    async def _main():
        timeout = aiohttp.ClientTimeout(total=160, connect=40, sock_read=60)
        connector = aiohttp.TCPConnector(limit=cfg.defaults.http_connection_limit)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            api = NomadApiClient(cfg, session)

            # Initialize database manager
            log_db = PipelineLogDB("pipeline_log.db")
            await log_db.initialize()

            monitor = NomadJobMonitor(api, log_db)
            await monitor.start()
            nomad = NomadService(api, monitor)

            data_svc = DataService(cfg)

            # Load geometry - either from WBD or mock data
            if args.use_mock_polygon:
                logging.info("Using polygon from mock data file")
                polygon_gdf = data_svc.load_polygon_gdf_from_file(cfg.mock_data_paths.polygon_data_file)
                # Use HUC code from config for mock data
                huc_code = cfg.mock_data_paths.huc or f"mock_{args.index}"
                pid = huc_code
            else:
                logging.info("Using polygon from WBD National gpkg")
                polygon_gdf, huc_code = data_svc.load_geometry_from_wbd(
                    cfg.wbd.gpkg_path, cfg.wbd.huc_list_path, args.index
                )
                pid = huc_code

            pipeline = PolygonPipeline(cfg, nomad, data_svc, polygon_gdf, pid, log_db)

            try:
                result = await pipeline.run()
                print(json.dumps(result, indent=2))
            finally:
                await pipeline.cleanup()
                await monitor.stop()
                await log_db.close()
                data_svc.cleanup()

    asyncio.run(_main())
