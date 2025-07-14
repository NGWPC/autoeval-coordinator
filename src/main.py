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
        tags: Dict[str, str],
        outputs_path: str,
        log_db: Optional[PipelineLogDB] = None,
    ):
        self.config = config
        self.nomad = nomad
        self.data_svc = data_svc
        self.polygon_gdf = polygon_gdf
        self.pipeline_id = pipeline_id
        self.tags = tags
        self.log_db = log_db
        # Ensure the temp directory exists and handle pipeline_id with path separators
        temp_dir = "/tmp"
        os.makedirs(temp_dir, exist_ok=True)
        self.tags_str = self.create_tags_str()
        self.tmp = tempfile.TemporaryDirectory(prefix=f"{os.path.basename(self.pipeline_id)}-", dir=temp_dir)

        self.path_factory = PathFactory(config, pipeline_id, outputs_path)

        # Populated by initialize()
        self.catchments: Dict[str, Dict[str, Any]] = {}
        self.flow_scenarios: Dict[str, Dict[str, str]] = {}
        self.benchmark_scenarios: Dict[str, Dict[str, List[str]]] = {}

    def create_tags_str(self) -> str:
        """
        Format tags as a string: [key1=val1,key2=val2,...]
        If 'batch_name' and 'aoi_name' are present, they appear first and second respectively.
        This is done to allow prefix searching in nomad job list.
        """
        ordered_keys = []
        if "batch_name" in self.tags:
            ordered_keys.append("batch_name")
        if "aoi_name" in self.tags:
            ordered_keys.append("aoi_name")
        ordered_keys += [k for k in self.tags if k not in ("batch_name", "aoi_name")]
        tag_str = ",".join(f"{k}={self.tags[k]}" for k in ordered_keys)
        return f"[{tag_str}]"

    async def initialize(self) -> None:
        """Query for catchments and flow scenarios."""
        # Clean up any stale jobs from previous runs if db_manager is available
        if self.log_db:
            await self.log_db.cleanup_pipeline_jobs(self.pipeline_id)
        # Query STAC for flow scenarios (always required)
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
            logger.debug(f"[{self.pipeline_id}] Found {len(self.flow_scenarios)} collections")

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
                self.config,
                self.nomad,
                self.data_svc,
                self.path_factory,
                self.pipeline_id,
                self.tags_str,
                self.catchments,
            )
            mosaic_stage = MosaicStage(
                self.config, self.nomad, self.data_svc, self.path_factory, self.pipeline_id, self.tags_str
            )
            agreement_stage = AgreementStage(
                self.config, self.nomad, self.data_svc, self.path_factory, self.pipeline_id, self.tags_str
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


def parsed_tags(tag_list):
    tags = {}
    core_tag_keys = {"batch_name", "aoi_name", "bench_src", "scenario"}

    for tag in tag_list:
        if "=" not in tag:
            raise argparse.ArgumentTypeError(f"Invalid tag format: '{tag}'. Expected key=value.")
        key, value = tag.split("=", 1)
        tags[key] = value

    # Check length of non-core tags
    non_core_tags = {k: v for k, v in tags.items() if k not in core_tag_keys}
    if non_core_tags:
        non_core_str = ",".join(f"{k}={v}" for k, v in non_core_tags.items())
        if len(non_core_str) > 70:
            raise argparse.ArgumentTypeError(
                f"Non-core tags exceed 70 character limit ({len(non_core_str)} chars): {non_core_str}"
            )

    return tags


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run one PolygonPipeline in isolation")
    parser.add_argument(
        "--aoi",
        type=str,
        required=True,
        help="File path to a GPKG containing a single polygon. If more than one layer/feature, only the first is used.",
    )
    parser.add_argument("--outputs_path", type=str, required=True, help="Output directory path")
    parser.add_argument(
        "--benchmark_sources",
        type=str,
        default=None,
        help="Comma-separated list of STAC collections to query (e.g., 'ble-collection,nwm-collection'). Defaults to all available sources.",
    )
    parser.add_argument("--hand_index_path", type=str, required=True, help="Path to HAND index data (required)")

    parser.add_argument(
        "--tags",
        nargs="*",
        type=str,
        default=[],
        help="List of key=value pairs for tagging (e.g., --tags batch=my_batch aoi=texas) These tags are included in job_ids that the pipeline will dispatch.",
    )

    args = parser.parse_args()

    if args.tags and args.tags != [""]:
        # Flatten any space-separated arguments to handle both manual and Nomad invocation styles
        flattened_tags = []
        for tag in args.tags:
            if " " in tag:
                flattened_tags.extend(tag.split())
            else:
                flattened_tags.append(tag)
        args.tags = parsed_tags(flattened_tags)
    else:
        args.tags = {}

    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    cfg = load_config()

    async def _main():
        # Validate AOI file extension
        if not args.aoi.lower().endswith(".gpkg"):
            raise ValueError(f"AOI file must be a GPKG file, got: {args.aoi}")

        outputs_path = args.outputs_path

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

            # if no benchmark collections provided all collections queried
            benchmark_collections = None
            if args.benchmark_sources:
                benchmark_collections = [col.strip() for col in args.benchmark_sources.split(",")]
                logging.info(f"Using benchmark sources: {benchmark_collections}")

            data_svc = DataService(cfg, args.hand_index_path, benchmark_collections)

            logging.info(f"Loading polygon from: {args.aoi}")
            polygon_gdf = data_svc.load_polygon_gdf_from_file(args.aoi)

            # Validate GPKG contents
            if len(polygon_gdf) == 0:
                raise ValueError("GPKG file contains no features")

            if len(polygon_gdf) > 1:
                logging.warning(f"Found {len(polygon_gdf)} features in {args.aoi}, using only the first one")
                polygon_gdf = polygon_gdf.iloc[[0]]

            geom = polygon_gdf.geometry.iloc[0]
            if geom.geom_type != "Polygon":
                raise ValueError(f"Feature must be POLYGON type, got: {geom.geom_type}")

            pipeline_id = os.environ.get("NOMAD_PIPELINE_JOB_ID", "test_pipeline_run")

            logging.info(f"Using HAND index path: {args.hand_index_path}")

            pipeline = PolygonPipeline(cfg, nomad, data_svc, polygon_gdf, pipeline_id, args.tags, outputs_path, log_db)

            try:
                result = await pipeline.run()
                print(json.dumps(result, indent=2))
            finally:
                await pipeline.cleanup()
                await nomad.stop()
                await log_db.close()
                data_svc.cleanup()

    asyncio.run(_main())
