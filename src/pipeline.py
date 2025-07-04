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
from load_config import AppConfig, load_config
from nomad_job_manager import NomadJobManager
from pipeline_log_db import PipelineLogDB
from pipeline_stages import AgreementStage, InundationStage, MosaicStage
from pipeline_utils import PathFactory, PipelineResult

logger = logging.getLogger(__name__)


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
        nomad: NomadJobManager,
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
            inundation_stage = InundationStage(
                self.config, self.nomad, self.data_svc, self.path_factory, self.pipeline_id, self.catchments
            )
            mosaic_stage = MosaicStage(self.config, self.nomad, self.data_svc, self.path_factory, self.pipeline_id)
            agreement_stage = AgreementStage(
                self.config, self.nomad, self.data_svc, self.path_factory, self.pipeline_id
            )

            results = await inundation_stage.run(results)
            results = await mosaic_stage.run(results)
            results = await agreement_stage.run(results)

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
            log_db = PipelineLogDB("pipeline_log.db")
            await log_db.initialize()

            nomad = NomadJobManager(
                nomad_addr=cfg.nomad.address,
                namespace=cfg.nomad.namespace,
                token=cfg.nomad.token,
                session=session,
                log_db=log_db,
            )
            await nomad.start()

            data_svc = DataService(cfg)

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
                await nomad.stop()
                await log_db.close()
                data_svc.cleanup()

    asyncio.run(_main())
