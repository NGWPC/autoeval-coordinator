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


class Result(BaseModel):
    pipeline_id: str
    polygon_id: str
    scenarios: List[ScenarioResult]
    catchment_count: int
    total_scenarios: int


class PolygonPipeline:
    """
    Enhanced pipeline that processes multiple flow scenarios:
    1) Query STAC for flow scenarios
    2) Combine flowfiles for each scenario
    3) Query hand index for catchments
    4) For each scenario:
       - For each catchment: dispatch inundator with scenario flowfile
       - Create mosaicked extent for scenario
    """

    def __init__(
        self,
        config: AppConfig,
        nomad: NomadService,
        data_svc: DataService,
        polygon: Dict[str, Any],
        pipeline_id: str,
    ):
        self.config = config
        self.nomad = nomad
        self.data_svc = data_svc
        self.polygon = polygon
        self.pipeline_id = pipeline_id
        # temp dir auto‐cleaned on .cleanup()
        self.tmp = tempfile.TemporaryDirectory(prefix=f"{pipeline_id}-")

        # Filled by initialize()
        self.catchments: Dict[str, Dict[str, Any]] = {}
        self.flow_scenarios: Dict[str, Dict[str, str]] = {}

    async def initialize(self) -> None:
        """Initialize pipeline by querying for catchments and flow scenarios."""
        # Step 1: Query STAC for flow scenarios (if enabled or mock data available)
        if (self.config.stac and self.config.stac.enabled) or self.config.mock_data_paths.mock_stac_results:
            logger.info(f"[{self.pipeline_id}] Querying STAC for flow scenarios")
            stac_data = await self.data_svc.query_stac_for_flow_scenarios(self.polygon)

            if stac_data.get("combined_flowfiles"):
                self.flow_scenarios = stac_data["combined_flowfiles"]
                logger.info(
                    f"[{self.pipeline_id}] Found {len(self.flow_scenarios)} collections with flow scenarios"
                    + (" (from mock data)" if stac_data.get("mock_data") else "")
                )
            else:
                logger.info(f"[{self.pipeline_id}] No flow scenarios found from STAC")

        # If no STAC scenarios found, raise error
        if not self.flow_scenarios:
            raise RuntimeError(
                f"[{self.pipeline_id}] No flow scenarios found. "
                "Please enable STAC querying or provide mock STAC results."
            )

        # Step 2: Query hand index for catchments
        logger.info(f"[{self.pipeline_id}] Querying hand index for catchments")
        data = await self.data_svc.query_for_catchments(self.polygon)
        self.catchments = data.get("catchments", {})
        if not self.catchments:
            raise RuntimeError(f"[{self.pipeline_id}] no catchments found")

        logger.info(
            f"[{self.pipeline_id}] Initialization complete: {len(self.catchments)} catchments, "
            f"{sum(len(scenarios) for scenarios in self.flow_scenarios.values())} flow scenarios"
        )

    async def run(self) -> Result:
        """Run the multi-scenario pipeline with stage-based parallelism."""
        # 1) Initialize
        await self.initialize()

        # 2) Prepare scenario metadata
        scenario_info = []
        scenario_counter = 0
        
        for collection_name, scenarios in self.flow_scenarios.items():
            for scenario_name, flowfile_path in scenarios.items():
                scenario_counter += 1
                scenario_id = f"{self.pipeline_id}-{collection_name}-{scenario_name}-{scenario_counter}"
                scenario_info.append({
                    'scenario_id': scenario_id,
                    'collection_name': collection_name,
                    'scenario_name': scenario_name,
                    'flowfile_path': flowfile_path,
                    'scenario_counter': scenario_counter
                })

        logger.info(f"[{self.pipeline_id}] Processing {len(scenario_info)} scenarios with stage-based parallelism")

        # 3) Stage 1: Submit all inundation jobs for all scenarios concurrently
        logger.info(f"[{self.pipeline_id}] Stage 1: Starting inundation jobs for all scenarios")
        inundation_results = await self._process_all_inundation_jobs(scenario_info)

        # 4) Stage 2: Submit all mosaic jobs for scenarios with valid inundation outputs
        logger.info(f"[{self.pipeline_id}] Stage 2: Starting mosaic jobs for scenarios with valid outputs")
        scenario_results = await self._process_all_mosaic_jobs(scenario_info, inundation_results)

        return Result(
            pipeline_id=self.pipeline_id,
            polygon_id=self.polygon.get("polygon_id", "unknown"),
            scenarios=scenario_results,
            catchment_count=len(self.catchments),
            total_scenarios=len(scenario_results),
        )

    async def _process_all_inundation_jobs(self, scenario_info: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """
        Stage 1: Process inundation jobs for all scenarios concurrently.
        Returns dict mapping scenario_id to list of valid inundation output paths.
        """
        # Collect all inundation tasks across all scenarios
        all_tasks = []
        scenario_task_mapping = {}  # Map scenario_id to list of (catch_id, task_index)
        
        task_index = 0
        for scenario in scenario_info:
            scenario_id = scenario['scenario_id']
            flowfile_path = scenario['flowfile_path']
            scenario_task_mapping[scenario_id] = []
            
            logger.info(f"[{scenario_id}] Creating inundation tasks for {len(self.catchments)} catchments")
            
            for catch_id, info in self.catchments.items():
                # Create a container to collect this specific task's output
                task_output_container = []
                task = asyncio.create_task(
                    self._process_catchment_for_scenario(scenario_id, catch_id, info, flowfile_path, task_output_container)
                )
                all_tasks.append((task, task_output_container, scenario_id, catch_id))
                scenario_task_mapping[scenario_id].append((catch_id, task_index))
                task_index += 1
        
        logger.info(f"[{self.pipeline_id}] Submitting {len(all_tasks)} inundation jobs across all scenarios")
        
        # Wait for all inundation tasks
        tasks_only = [task_data[0] for task_data in all_tasks]
        results = await asyncio.gather(*tasks_only, return_exceptions=True)
        
        # Process results and group by scenario
        inundation_results = {}
        total_failed = 0
        
        for i, (result, (task, output_container, scenario_id, catch_id)) in enumerate(zip(results, all_tasks)):
            if scenario_id not in inundation_results:
                inundation_results[scenario_id] = []
            
            if isinstance(result, Exception):
                total_failed += 1
                logger.error(f"[{scenario_id}] Catchment {catch_id} inundation failed: {result}")
            else:
                # Task succeeded, add outputs to scenario results
                inundation_results[scenario_id].extend(output_container)
        
        logger.info(f"[{self.pipeline_id}] Inundation stage complete: {total_failed}/{len(all_tasks)} jobs failed")
        
        # Validate outputs for each scenario
        validated_results = {}
        for scenario_id, outputs in inundation_results.items():
            if outputs:
                valid_outputs = await self.data_svc.validate_s3_files(outputs)
                validated_results[scenario_id] = valid_outputs
                logger.info(f"[{scenario_id}] {len(valid_outputs)}/{len(outputs)} inundation outputs are valid")
            else:
                validated_results[scenario_id] = []
                logger.warning(f"[{scenario_id}] No successful inundation outputs")
        
        return validated_results

    async def _process_all_mosaic_jobs(self, scenario_info: List[Dict[str, Any]], inundation_results: Dict[str, List[str]]) -> List[ScenarioResult]:
        """
        Stage 2: Process mosaic jobs for all scenarios with valid inundation outputs.
        """
        mosaic_tasks = []
        valid_scenarios = []
        
        for scenario in scenario_info:
            scenario_id = scenario['scenario_id']
            valid_outputs = inundation_results.get(scenario_id, [])
            
            if not valid_outputs:
                logger.warning(f"[{scenario_id}] Skipping mosaic - no valid inundation outputs")
                continue
            
            logger.info(f"[{scenario_id}] Creating mosaic task with {len(valid_outputs)} inundation outputs")
            
            mosaic_output_path = (
                f"s3://{self.config.s3.bucket}"
                f"/{self.config.s3.base_prefix}"
                f"/pipeline_{self.pipeline_id}/scenario_{scenario_id}/HAND_mosaic.tif"
            )
            
            mosaic_meta = MosaicDispatchMeta(
                pipeline_id=scenario_id,
                raster_paths=valid_outputs,
                output_path=mosaic_output_path,
                fim_type=self.config.defaults.fim_type,
                gdal_cache_max=str(self.config.defaults.gdal_cache_max),
                registry_token=self.config.nomad.registry_token or "",
                aws_access_key=self.config.s3.AWS_ACCESS_KEY_ID or "",
                aws_secret_key=self.config.s3.AWS_SECRET_ACCESS_KEY or "",
                aws_session_token=self.config.s3.AWS_SESSION_TOKEN or "",
            )
            
            task = asyncio.create_task(
                self.nomad.run_job(
                    self.config.jobs.fim_mosaicker,
                    instance_prefix=f"mosaic-{scenario_id[:16]}",
                    meta=mosaic_meta.model_dump(),
                )
            )
            mosaic_tasks.append(task)
            valid_scenarios.append(scenario)
        
        if not mosaic_tasks:
            logger.warning(f"[{self.pipeline_id}] No mosaic jobs to submit - all scenarios failed inundation")
            return []
        
        logger.info(f"[{self.pipeline_id}] Submitting {len(mosaic_tasks)} mosaic jobs")
        
        # Wait for all mosaic tasks
        mosaic_results = await asyncio.gather(*mosaic_tasks, return_exceptions=True)
        
        # Build final scenario results
        scenario_results = []
        for i, (result, scenario) in enumerate(zip(mosaic_results, valid_scenarios)):
            scenario_id = scenario['scenario_id']
            
            if isinstance(result, Exception):
                logger.error(f"[{scenario_id}] Mosaic failed: {result}")
            else:
                logger.info(f"[{scenario_id}] Scenario complete → {result}")
                scenario_results.append(ScenarioResult(
                    scenario_id=scenario_id,
                    collection_name=scenario['collection_name'],
                    scenario_name=scenario['scenario_name'],
                    flowfile_path=scenario['flowfile_path'],
                    mosaic_output=result,
                ))
        
        logger.info(f"[{self.pipeline_id}] Pipeline complete: {len(scenario_results)}/{len(scenario_info)} scenarios succeeded")
        return scenario_results

    async def _process_scenario(
        self,
        scenario_id: str,
        collection_name: str,
        scenario_name: str,
        flowfile_path: str,
    ) -> ScenarioResult:
        """
        Process a single flow scenario:
        1) Run inundation for all catchments with this flowfile
        2) Create mosaic from all inundation outputs
        """
        logger.info(f"[{scenario_id}] Starting scenario processing")

        # 1) Launch all inundator jobs for this scenario concurrently
        inund_outputs: List[str] = []
        tasks = []
        for catch_id, info in self.catchments.items():
            task = asyncio.create_task(
                self._process_catchment_for_scenario(scenario_id, catch_id, info, flowfile_path, inund_outputs)
            )
            tasks.append(task)
        
        # Wait for all tasks and collect results, allowing some to fail
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Log any failed tasks
        failed_count = 0
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                failed_count += 1
                catch_id = list(self.catchments.keys())[i]
                logger.error(f"[{scenario_id}] Catchment {catch_id} failed: {result}")
        
        if failed_count > 0:
            logger.warning(f"[{scenario_id}] {failed_count}/{len(tasks)} catchment jobs failed")
        
        if failed_count == len(tasks):
            raise RuntimeError(f"[{scenario_id}] All catchment jobs failed")

        # 2) Validate S3 outputs before mosaicking
        valid_outputs = await self.data_svc.validate_s3_files(inund_outputs)
        if not valid_outputs:
            raise RuntimeError(f"[{scenario_id}] No valid inundation outputs found for mosaicking")

        if len(valid_outputs) < len(inund_outputs):
            logger.info(f"[{scenario_id}] Only {len(valid_outputs)}/{len(inund_outputs)} inundation outputs are valid")

        # 3) Dispatch & await mosaicker for this scenario
        mosaic_output_path = (
            f"s3://{self.config.s3.bucket}"
            f"/{self.config.s3.base_prefix}"
            f"/pipeline_{self.pipeline_id}/scenario_{scenario_id}/HAND_mosaic.tif"
        )

        mosaic_meta = MosaicDispatchMeta(
            pipeline_id=scenario_id,
            raster_paths=valid_outputs,
            output_path=mosaic_output_path,
            fim_type=self.config.defaults.fim_type,
            gdal_cache_max=str(self.config.defaults.gdal_cache_max),
            registry_token=self.config.nomad.registry_token or "",
            aws_access_key=self.config.s3.AWS_ACCESS_KEY_ID or "",
            aws_secret_key=self.config.s3.AWS_SECRET_ACCESS_KEY or "",
            aws_session_token=self.config.s3.AWS_SESSION_TOKEN or "",
        )

        mosaic_out = await self.nomad.run_job(
            self.config.jobs.fim_mosaicker,
            instance_prefix=f"mosaic-{scenario_id[:16]}",
            meta=mosaic_meta.model_dump(),
        )

        logger.info(f"[{scenario_id}] Scenario complete → {mosaic_out}")

        return ScenarioResult(
            scenario_id=scenario_id,
            collection_name=collection_name,
            scenario_name=scenario_name,
            flowfile_path=flowfile_path,
            mosaic_output=mosaic_out,
        )

    async def _process_catchment_for_scenario(
        self,
        scenario_id: str,
        catch_id: str,
        catchment_info: Dict[str, Any],
        flowfile_path: str,
        collector: List[str],
    ) -> None:
        """
        Process a single catchment for a specific scenario:
        1) Copy parquet file to S3 if needed
        2) Dispatch inundator with scenario-specific flowfile
        3) Await its output
        """
        base = (
            f"s3://{self.config.s3.bucket}"
            f"/{self.config.s3.base_prefix}"
            f"/pipeline_{self.pipeline_id}/scenario_{scenario_id}/catchment_{catch_id}"
        )

        # Get the local parquet path from info
        local_parquet_path = catchment_info.get("parquet_path")
        if not local_parquet_path:
            raise ValueError(f"No parquet_path found for catchment {catch_id}")

        # Copy parquet to S3 if it's local
        s3_parquet_path = f"{base}/catchment_data.parquet"
        parquet_path = await self.data_svc.copy_file_to_uri(local_parquet_path, s3_parquet_path)

        # Copy flowfile to S3 if it's local
        s3_flowfile_path = f"{base}/flowfile.csv"
        scenario_flowfile_path = await self.data_svc.copy_file_to_uri(flowfile_path, s3_flowfile_path)

        meta = InundationDispatchMeta(
            pipeline_id=scenario_id,
            catchment_data_path=parquet_path,
            forecast_path=scenario_flowfile_path,  # Use scenario-specific flowfile
            output_path=f"{base}/inundation_output.tif",
            fim_type=self.config.defaults.fim_type,
            gdal_cache_max=str(self.config.defaults.gdal_cache_max),
            registry_token=self.config.nomad.registry_token or "",
            aws_access_key=self.config.s3.AWS_ACCESS_KEY_ID or "",
            aws_secret_key=self.config.s3.AWS_SECRET_ACCESS_KEY or "",
            aws_session_token=self.config.s3.AWS_SESSION_TOKEN or "",
        )

        logger.debug(
            f"[{scenario_id}/{catch_id}] using catchment parquet → {parquet_path}, "
            f"flowfile → {scenario_flowfile_path}"
        )

        # Dispatch & await inundator
        out = await self.nomad.run_job(
            self.config.jobs.hand_inundator,
            instance_prefix=f"inund-{scenario_id[:12]}-{str(catch_id)[:8]}",
            meta=meta.model_dump(),
        )
        collector.append(out)
        logger.debug(f"[{scenario_id}/{catch_id}] inundator done → {out}")

    async def cleanup(self) -> None:
        """Remove tempdir and clean up resources."""
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
    parser.add_argument(
        "--polys",
        help="JSON file containing a list of polygon dicts",
    )
    parser.add_argument(
        "--index",
        type=int,
        default=0,
        help="Which polygon in the list to process",
    )
    parser.add_argument(
        "--config",
        default=os.path.join("config", "pipeline_config.yml"),
        help="Path to your YAML config",
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
