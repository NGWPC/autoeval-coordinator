import asyncio
import logging
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel

from data_service import DataService
from load_config import AppConfig
from nomad_service import (
    AgreementDispatchMeta,
    InundationDispatchMeta,
    MosaicDispatchMeta,
    NomadService,
)

logger = logging.getLogger(__name__)


class ScenarioResult(BaseModel):
    scenario_id: str
    collection_name: str
    scenario_name: str
    flowfile_path: str
    mosaic_output: str
    benchmark_mosaic_output: str
    agreement_output: str
    agreement_metrics_path: str


class Result(BaseModel):
    pipeline_id: str
    polygon_id: str
    scenarios: List[ScenarioResult]
    catchment_count: int
    total_scenarios: int


class PolygonPipeline:
    """
    Pipeline that processes multiple flow scenarios with stage-based parallelism:
    1) Initialize: Query STAC for scenarios and hand index for catchments
    2) Stage 1: Run all inundation jobs concurrently across all scenarios
    3) Stage 2: Run all mosaic jobs concurrently for scenarios with valid outputs
    """

    def __init__(
        self, config: AppConfig, nomad: NomadService, data_svc: DataService, polygon: Dict[str, Any], pipeline_id: str
    ):
        self.config = config
        self.nomad = nomad
        self.data_svc = data_svc
        self.polygon = polygon
        self.pipeline_id = pipeline_id
        self.tmp = tempfile.TemporaryDirectory(prefix=f"{pipeline_id}-")

        # Populated by initialize()
        self.catchments: Dict[str, Dict[str, Any]] = {}
        self.flow_scenarios: Dict[str, Dict[str, str]] = {}
        self.benchmark_scenarios: Dict[str, Dict[str, List[str]]] = {}

    async def initialize(self) -> None:
        """Query for catchments and flow scenarios."""
        # Query STAC for flow scenarios
        if (self.config.stac and self.config.stac.enabled) or self.config.mock_data_paths.mock_stac_results:
            logger.info(f"[{self.pipeline_id}] Querying STAC for flow scenarios")
            stac_data = await self.data_svc.query_stac_for_flow_scenarios(self.polygon)
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
                logger.info(
                    f"[{self.pipeline_id}] Found {len(self.flow_scenarios)} collections"
                    + (" (from mock data)" if stac_data.get("mock_data") else "")
                )

        if not self.flow_scenarios:
            raise RuntimeError(f"[{self.pipeline_id}] No flow scenarios found")

        # Query hand index for catchments
        logger.info(f"[{self.pipeline_id}] Querying hand index for catchments")
        data = await self.data_svc.query_for_catchments(self.polygon)
        self.catchments = data.get("catchments", {})

        if not self.catchments:
            raise RuntimeError(f"[{self.pipeline_id}] No catchments found")

        total_scenarios = sum(len(scenarios) for scenarios in self.flow_scenarios.values())
        logger.info(
            f"[{self.pipeline_id}] Initialization complete: {len(self.catchments)} catchments, "
            f"{total_scenarios} flow scenarios"
        )

    async def run(self) -> Result:
        """Run the pipeline with stage-based parallelism."""
        await self.initialize()

        # Build scenario list
        scenarios = []
        for i, (collection, flows) in enumerate(self.flow_scenarios.items()):
            for scenario, flowfile_path in flows.items():
                benchmark_rasters = self.benchmark_scenarios.get(collection, {}).get(scenario, [])
                scenarios.append(
                    {
                        "scenario_id": f"{self.pipeline_id}-{collection}-{scenario}-{i+1}",
                        "collection_name": collection,
                        "scenario_name": scenario,
                        "flowfile_path": flowfile_path,
                        "benchmark_rasters": benchmark_rasters,
                    }
                )

        logger.info(f"[{self.pipeline_id}] Processing {len(scenarios)} scenarios with stage-based parallelism")

        # Stage 1: All inundation jobs
        logger.info(f"[{self.pipeline_id}] Stage 1: Starting inundation jobs for all scenarios")
        inundation_results = await self._run_inundation_stage(scenarios)

        # Stage 2: All mosaic jobs
        logger.info(f"[{self.pipeline_id}] Stage 2: Starting mosaic jobs for scenarios with valid outputs")
        mosaic_results = await self._run_mosaic_stage(scenarios, inundation_results)

        # Stage 3: All agreement jobs
        logger.info(f"[{self.pipeline_id}] Stage 3: Starting agreement jobs for scenarios with valid mosaics")
        scenario_results = await self._run_agreement_stage(mosaic_results)

        return Result(
            pipeline_id=self.pipeline_id,
            polygon_id=self.polygon.get("polygon_id", "unknown"),
            scenarios=scenario_results,
            catchment_count=len(self.catchments),
            total_scenarios=len(scenario_results),
        )

    async def _run_inundation_stage(self, scenarios: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """Run all inundation jobs concurrently and return valid outputs by scenario."""
        # Create all inundation tasks
        tasks = []
        for scenario in scenarios:
            for catch_id, info in self.catchments.items():
                output_container = []
                task = asyncio.create_task(self._process_catchment(scenario, catch_id, info, output_container))
                tasks.append((task, output_container, scenario["scenario_id"], catch_id))

        logger.info(f"[{self.pipeline_id}] Submitting {len(tasks)} inundation jobs across all scenarios")

        # Wait for all tasks
        results = await asyncio.gather(*[t[0] for t in tasks], return_exceptions=True)

        # Group outputs by scenario
        outputs_by_scenario = {}
        failed_count = 0

        for (task, output_container, scenario_id, catch_id), result in zip(tasks, results):
            if scenario_id not in outputs_by_scenario:
                outputs_by_scenario[scenario_id] = []

            if isinstance(result, Exception):
                failed_count += 1
                logger.error(f"[{scenario_id}] Catchment {catch_id} inundation failed: {result}")
            else:
                outputs_by_scenario[scenario_id].extend(output_container)

        logger.info(f"[{self.pipeline_id}] Inundation stage complete: {failed_count}/{len(tasks)} jobs failed")

        # Validate outputs for each scenario
        validated_results = {}
        for scenario_id, outputs in outputs_by_scenario.items():
            valid_outputs = await self.data_svc.validate_s3_files(outputs) if outputs else []
            validated_results[scenario_id] = valid_outputs
            logger.info(f"[{scenario_id}] {len(valid_outputs)}/{len(outputs)} inundation outputs are valid")

        return validated_results

    async def _run_mosaic_stage(
        self, scenarios: List[Dict[str, Any]], inundation_results: Dict[str, List[str]]
    ) -> List[Dict[str, Any]]:
        """Run mosaic jobs for scenarios with valid outputs."""
        # Create mosaic tasks for scenarios with valid outputs
        hand_mosaic_tasks = []
        benchmark_mosaic_tasks = []
        valid_scenarios = []

        for scenario in scenarios:
            scenario_id = scenario["scenario_id"]
            valid_outputs = inundation_results.get(scenario_id, [])
            benchmark_rasters = scenario.get("benchmark_rasters", [])

            if not valid_outputs:
                logger.warning(f"[{scenario_id}] Skipping mosaic - no valid inundation outputs")
                continue

            if not benchmark_rasters:
                logger.warning(f"[{scenario_id}] Skipping benchmark mosaic - no benchmark rasters")
                continue

            logger.info(f"[{scenario_id}] Creating HAND mosaic task with {len(valid_outputs)} inundation outputs")
            logger.info(
                f"[{scenario_id}] Creating benchmark mosaic task with {len(benchmark_rasters)} benchmark rasters"
            )

            # HAND mosaic task
            hand_output_path = (
                f"s3://{self.config.s3.bucket}/{self.config.s3.base_prefix}/"
                f"pipeline_{self.pipeline_id}/scenario_{scenario_id}/HAND_mosaic.tif"
            )
            hand_meta = self._create_mosaic_meta(scenario_id, valid_outputs, hand_output_path)
            hand_task = asyncio.create_task(
                self.nomad.run_job(
                    self.config.jobs.fim_mosaicker,
                    instance_prefix=f"hand-mosaic-{scenario_id[:12]}",
                    meta=hand_meta.model_dump(),
                )
            )
            hand_mosaic_tasks.append(hand_task)

            # Benchmark mosaic task
            benchmark_output_path = (
                f"s3://{self.config.s3.bucket}/{self.config.s3.base_prefix}/"
                f"pipeline_{self.pipeline_id}/scenario_{scenario_id}/benchmark_mosaic.tif"
            )
            benchmark_meta = self._create_mosaic_meta(
                f"{scenario_id}-benchmark", benchmark_rasters, benchmark_output_path
            )
            benchmark_task = asyncio.create_task(
                self.nomad.run_job(
                    self.config.jobs.fim_mosaicker,
                    instance_prefix=f"bench-mosaic-{scenario_id[:12]}",
                    meta=benchmark_meta.model_dump(),
                )
            )
            benchmark_mosaic_tasks.append(benchmark_task)
            valid_scenarios.append(scenario)

        if not hand_mosaic_tasks:
            logger.warning(f"[{self.pipeline_id}] No mosaic jobs to submit - all scenarios failed inundation")
            return []

        total_mosaic_jobs = len(hand_mosaic_tasks) + len(benchmark_mosaic_tasks)
        logger.info(
            f"[{self.pipeline_id}] Submitting {total_mosaic_jobs} mosaic jobs ({len(hand_mosaic_tasks)} HAND, {len(benchmark_mosaic_tasks)} benchmark)"
        )

        # Wait for all mosaic tasks
        hand_results = await asyncio.gather(*hand_mosaic_tasks, return_exceptions=True)
        benchmark_results = await asyncio.gather(*benchmark_mosaic_tasks, return_exceptions=True)

        # Build intermediate results for agreement stage
        mosaic_results = []
        for hand_result, benchmark_result, scenario in zip(hand_results, benchmark_results, valid_scenarios):
            scenario_id = scenario["scenario_id"]

            # Check if both mosaics succeeded
            hand_failed = isinstance(hand_result, Exception)
            benchmark_failed = isinstance(benchmark_result, Exception)

            if hand_failed:
                logger.error(f"[{scenario_id}] HAND mosaic failed: {hand_result}")
            if benchmark_failed:
                logger.error(f"[{scenario_id}] Benchmark mosaic failed: {benchmark_result}")

            # Only create result if both mosaics succeeded
            if not hand_failed and not benchmark_failed:
                logger.info(
                    f"[{scenario_id}] Mosaic stage complete → HAND: {hand_result}, Benchmark: {benchmark_result}"
                )
                mosaic_results.append(
                    {
                        **scenario,  # Include original scenario data
                        "mosaic_output": hand_result,
                        "benchmark_mosaic_output": benchmark_result,
                    }
                )
            else:
                logger.warning(f"[{scenario_id}] Scenario failed - one or both mosaics failed")

        logger.info(
            f"[{self.pipeline_id}] Mosaic stage complete: {len(mosaic_results)}/{len(scenarios)} scenarios succeeded"
        )
        return mosaic_results

    async def _run_agreement_stage(self, mosaic_results: List[Dict[str, Any]]) -> List[ScenarioResult]:
        """Run agreement jobs for scenarios with valid mosaic outputs."""
        if not mosaic_results:
            logger.warning(f"[{self.pipeline_id}] No agreement jobs to submit - all scenarios failed mosaicking")
            return []

        # Create agreement tasks
        agreement_tasks = []
        for scenario in mosaic_results:
            scenario_id = scenario["scenario_id"]

            # Create agreement output paths
            agreement_output_path = (
                f"s3://{self.config.s3.bucket}/{self.config.s3.base_prefix}/"
                f"pipeline_{self.pipeline_id}/scenario_{scenario_id}/agreement_map.tif"
            )

            # Create metrics output path
            metrics_output_path = (
                f"s3://{self.config.s3.bucket}/{self.config.s3.base_prefix}/"
                f"pipeline_{self.pipeline_id}/scenario_{scenario_id}/agreement_metrics.csv"
            )

            # Create agreement job metadata
            meta = self._create_agreement_meta(
                scenario_id,
                scenario["mosaic_output"],  # candidate (HAND mosaic)
                scenario["benchmark_mosaic_output"],  # benchmark mosaic
                agreement_output_path,
                metrics_output_path,
            )

            task = asyncio.create_task(
                self.nomad.run_job(
                    self.config.jobs.agreement_maker,
                    instance_prefix=f"agree-{scenario_id[:12]}",
                    meta=meta.model_dump(),
                )
            )
            agreement_tasks.append(task)

        logger.info(f"[{self.pipeline_id}] Submitting {len(agreement_tasks)} agreement jobs")

        # Wait for all agreement tasks
        agreement_results = await asyncio.gather(*agreement_tasks, return_exceptions=True)

        # Build final scenario results
        scenario_results = []
        for agreement_result, scenario in zip(agreement_results, mosaic_results):
            scenario_id = scenario["scenario_id"]

            # Check if agreement job succeeded
            agreement_failed = isinstance(agreement_result, Exception)

            if agreement_failed:
                logger.error(f"[{scenario_id}] Agreement job failed: {agreement_result}")
                continue

            # Construct metrics path (same pattern as agreement output)
            metrics_path = (
                f"s3://{self.config.s3.bucket}/{self.config.s3.base_prefix}/"
                f"pipeline_{self.pipeline_id}/scenario_{scenario_id}/agreement_metrics.csv"
            )

            logger.info(f"[{scenario_id}] Pipeline complete → Agreement: {agreement_result}")
            scenario_results.append(
                ScenarioResult(
                    scenario_id=scenario_id,
                    collection_name=scenario["collection_name"],
                    scenario_name=scenario["scenario_name"],
                    flowfile_path=scenario["flowfile_path"],
                    mosaic_output=scenario["mosaic_output"],
                    benchmark_mosaic_output=scenario["benchmark_mosaic_output"],
                    agreement_output=agreement_result,
                    agreement_metrics_path=metrics_path,
                )
            )

        logger.info(
            f"[{self.pipeline_id}] Pipeline complete: {len(scenario_results)}/{len(mosaic_results)} scenarios succeeded"
        )
        return scenario_results

    async def _process_catchment(
        self, scenario: Dict[str, Any], catch_id: str, catchment_info: Dict[str, Any], collector: List[str]
    ) -> None:
        """Process a single catchment for a scenario."""
        scenario_id = scenario["scenario_id"]
        flowfile_path = scenario["flowfile_path"]

        base_path = (
            f"s3://{self.config.s3.bucket}/{self.config.s3.base_prefix}/"
            f"pipeline_{self.pipeline_id}/scenario_{scenario_id}/catchment_{catch_id}"
        )

        # Copy files to S3
        local_parquet = catchment_info.get("parquet_path")
        if not local_parquet:
            raise ValueError(f"No parquet_path found for catchment {catch_id}")

        parquet_path = await self.data_svc.copy_file_to_uri(local_parquet, f"{base_path}/catchment_data.parquet")
        flowfile_s3_path = await self.data_svc.copy_file_to_uri(flowfile_path, f"{base_path}/flowfile.csv")

        logger.debug(
            f"[{scenario_id}/{catch_id}] using catchment parquet → {parquet_path}, " f"flowfile → {flowfile_s3_path}"
        )

        # Create job metadata and run
        meta = self._create_inundation_meta(
            scenario_id, parquet_path, flowfile_s3_path, f"{base_path}/inundation_output.tif"
        )

        output = await self.nomad.run_job(
            self.config.jobs.hand_inundator,
            instance_prefix=f"inund-{scenario_id[:12]}-{str(catch_id)[:8]}",
            meta=meta.model_dump(),
        )
        collector.append(output)
        logger.debug(f"[{scenario_id}/{catch_id}] inundator done → {output}")

    def _create_inundation_meta(
        self, pipeline_id: str, catchment_path: str, forecast_path: str, output_path: str
    ) -> InundationDispatchMeta:
        """Create inundation job metadata using existing DispatchMetaBase."""
        return InundationDispatchMeta(
            pipeline_id=pipeline_id,
            catchment_data_path=catchment_path,
            forecast_path=forecast_path,
            output_path=output_path,
            fim_type=self.config.defaults.fim_type,
            registry_token=self.config.nomad.registry_token or "",
            aws_access_key=self.config.s3.AWS_ACCESS_KEY_ID or "",
            aws_secret_key=self.config.s3.AWS_SECRET_ACCESS_KEY or "",
            aws_session_token=self.config.s3.AWS_SESSION_TOKEN or "",
        )

    def _create_mosaic_meta(self, pipeline_id: str, raster_paths: List[str], output_path: str) -> MosaicDispatchMeta:
        """Create mosaic job metadata using existing DispatchMetaBase."""
        return MosaicDispatchMeta(
            pipeline_id=pipeline_id,
            raster_paths=raster_paths,
            output_path=output_path,
            fim_type=self.config.defaults.fim_type,
            registry_token=self.config.nomad.registry_token or "",
            aws_access_key=self.config.s3.AWS_ACCESS_KEY_ID or "",
            aws_secret_key=self.config.s3.AWS_SECRET_ACCESS_KEY or "",
            aws_session_token=self.config.s3.AWS_SESSION_TOKEN or "",
        )

    def _create_agreement_meta(
        self, pipeline_id: str, candidate_path: str, benchmark_path: str, output_path: str, metrics_path: str = ""
    ) -> AgreementDispatchMeta:
        """Create agreement job metadata using existing DispatchMetaBase."""
        return AgreementDispatchMeta(
            pipeline_id=pipeline_id,
            candidate_path=candidate_path,
            benchmark_path=benchmark_path,
            output_path=output_path,
            metrics_path=metrics_path,
            fim_type=self.config.defaults.fim_type,
            registry_token=self.config.nomad.registry_token or "",
            aws_access_key=self.config.s3.AWS_ACCESS_KEY_ID or "",
            aws_secret_key=self.config.s3.AWS_SECRET_ACCESS_KEY or "",
            aws_session_token=self.config.s3.AWS_SESSION_TOKEN or "",
        )

    async def cleanup(self) -> None:
        """Clean up temporary resources."""
        self.tmp.cleanup()
        logger.info(f"[{self.pipeline_id}] cleaned up temp files")


if __name__ == "__main__":
    import argparse
    import json
    import os

    import aiohttp

    from job_monitor import NomadJobMonitor
    from load_config import load_config
    from nomad_api import NomadApiClient

    parser = argparse.ArgumentParser(description="Run one PolygonPipeline in isolation")
    parser.add_argument("--polys", help="JSON file containing a list of polygon dicts")
    parser.add_argument("--index", type=int, default=0, help="Which polygon in the list to process")
    parser.add_argument(
        "--config", default=os.path.join("config", "pipeline_config.yml"), help="Path to your YAML config"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    cfg = load_config(args.config)
    with open(args.polys, "r") as fp:
        polygons = json.load(fp)
    if not polygons:
        raise RuntimeError(f"No polygons found in {args.polys!r}")
    polygon = polygons[args.index]

    async def _main():
        timeout = aiohttp.ClientTimeout(total=160, connect=40, sock_read=60)
        connector = aiohttp.TCPConnector(limit=cfg.defaults.http_connection_limit)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            api = NomadApiClient(cfg, session)
            monitor = NomadJobMonitor(api)
            await monitor.start()
            nomad = NomadService(api, monitor)

            data_svc = DataService(cfg)
            pid = str(850)
            pipeline = PolygonPipeline(cfg, nomad, data_svc, polygon, pid)

            try:
                result = await pipeline.run()
                data = result.model_dump()
                print(json.dumps(data, indent=2))
            finally:
                await pipeline.cleanup()
                await monitor.stop()
                data_svc.cleanup()

    asyncio.run(_main())
