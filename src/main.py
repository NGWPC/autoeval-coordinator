import argparse
import asyncio
import json
import logging
import os
import tempfile
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from uuid import uuid4

import aiohttp
import fsspec
import geopandas as gpd

import default_config
from data_service import DataService
from load_config import AppConfig, load_config
from metrics_aggregator import MetricsAggregator
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
        tags: Dict[str, str],
        outputs_path: str,
        log_db: Optional[PipelineLogDB] = None,
    ):
        self.config = config
        self.nomad = nomad
        self.data_svc = data_svc
        self.polygon_gdf = polygon_gdf
        self.tags = tags
        self.log_db = log_db
        # Ensure the temp directory exists
        temp_dir = "/tmp"
        os.makedirs(temp_dir, exist_ok=True)
        # Store tags dict directly - pipeline stages will create tag strings as needed
        self.tmp = tempfile.TemporaryDirectory(prefix="pipeline-", dir=temp_dir)

        # Get aoi_name from tags for the PathFactory
        aoi_name = self.tags["aoi_name"]  # Required tag, will always exist
        self.path_factory = PathFactory(config, outputs_path, aoi_name)

        # Populated by initialize()
        self.catchments: Dict[str, Dict[str, Any]] = {}
        self.flow_scenarios: Dict[str, Dict[str, str]] = {}
        self.benchmark_scenarios: Dict[str, Dict[str, List[str]]] = {}
        self.stac_results: Dict[str, Dict[str, Dict[str, List[str]]]] = {}

    async def initialize(self) -> None:
        """Query for catchments and flow scenarios."""
        # Query STAC for flow scenarios (always required)
        logger.debug("Querying STAC for flow scenarios")
        stac_data = await self.data_svc.query_stac_for_flow_scenarios(self.polygon_gdf)
        self.flow_scenarios = stac_data.get("combined_flowfiles", {})

        # Extract benchmark rasters from STAC scenarios
        raw_scenarios = stac_data.get("scenarios", {})
        self.stac_results = raw_scenarios
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
            logger.debug(f"Found {len(self.flow_scenarios)} collections")

        if not self.flow_scenarios:
            raise RuntimeError("No flow scenarios found")

        # Query hand index for catchments
        logger.debug("Querying hand index for catchments")
        data = await self.data_svc.query_for_catchments(self.polygon_gdf)
        self.catchments = data.get("catchments", {})

        if not self.catchments:
            raise RuntimeError("No catchments found")

        total_scenarios = sum(len(scenarios) for scenarios in self.flow_scenarios.values())
        logger.info(f"Initialization complete: {len(self.catchments)} catchments, " f"{total_scenarios} flow scenarios")

    async def run(self) -> Dict[str, Any]:
        """Run the pipeline with stage-based parallelism."""
        await self.initialize()

        # Build scenario results
        results = []
        for collection, flows in self.flow_scenarios.items():
            for scenario, flowfile_path in flows.items():
                benchmark_rasters = self.benchmark_scenarios.get(collection, {}).get(scenario, [])
                result = PipelineResult(
                    scenario_id=f"{collection}-{scenario}",
                    collection_name=collection,
                    scenario_name=scenario,
                    flowfile_path=flowfile_path,
                    benchmark_rasters=benchmark_rasters,
                    metadata={
                        "autoeval_coordinator_version": default_config.AUTOEVAL_COORDINATOR_VERSION,
                        "hand_inundator_version": default_config.HAND_INUNDATOR_VERSION,
                        "hand_inundator_version": default_config.HAND_INUNDATOR_VERSION,
                        "hand_inundator_version": default_config.FIM_MOSAICKER_VERSION,
                        "batch_name": self.tags.get("batch_name", ""),
                        "aoi_name": self.tags.get("aoi_name", ""),
                    },
                )
                results.append(result)

        logger.debug(f"Processing {len(results)} scenarios with stage-based parallelism")

        try:
            inundation_stage = InundationStage(
                self.config,
                self.nomad,
                self.data_svc,
                self.path_factory,
                self.tags,
                self.catchments,
            )
            mosaic_stage = MosaicStage(self.config, self.nomad, self.data_svc, self.path_factory, self.tags)
            agreement_stage = AgreementStage(self.config, self.nomad, self.data_svc, self.path_factory, self.tags)

            results = await inundation_stage.run(results)
            results = await mosaic_stage.run(results)
            results = await agreement_stage.run(results)

            if results:
                try:
                    results_json_path = self.path_factory.results_json_path()
                    # Convert results to serializable format
                    serializable_results = []
                    for result in results:
                        result_dict = {
                            "scenario_id": result.scenario_id,
                            "collection_name": result.collection_name,
                            "scenario_name": result.scenario_name,
                            "flowfile_path": result.flowfile_path,
                            "benchmark_rasters": result.benchmark_rasters,
                            "status": result.status,
                            "paths": result.paths,
                            "metadata": result.metadata,
                            "error": result.error,
                        }
                        serializable_results.append(result_dict)

                    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as temp_file:
                        json.dump(serializable_results, temp_file, indent=2)
                        temp_json_path = temp_file.name

                    await self.data_svc.copy_file_to_uri(temp_json_path, results_json_path)
                    logger.info(f"Results JSON written to {results_json_path}")

                    if os.path.exists(temp_json_path):
                        os.unlink(temp_json_path)

                except Exception as e:
                    logger.error(f"Failed to write results JSON: {e}")

            if results:
                logger.info("Running metrics aggregation...")
                try:
                    aggregator = MetricsAggregator(
                        outputs_path=str(self.path_factory.base),
                        stac_results=self.stac_results,
                        data_service=self.data_svc,
                    )
                    metrics_path = aggregator.save_results(self.path_factory.metrics_path())
                    logger.info(f"Metrics aggregation completed: {metrics_path}")
                except Exception as e:
                    logger.error(f"Metrics aggregation failed: {e}")

            successful_results = [r for r in results if r.status == "completed"]
            total_attempted = len([r for r in results if r.status != "pending"])

            summary = {
                "status": "success",
                "catchment_count": len(self.catchments),
                "total_scenarios_attempted": total_attempted,
                "successful_scenarios": len(successful_results),
                "message": f"Pipeline completed successfully with {len(successful_results)}/{total_attempted} scenarios",
            }
            logger.info(f"Pipeline SUCCESS: {len(successful_results)}/{total_attempted} scenarios completed")
            return summary
        except Exception as e:
            logger.error(f"Pipeline FAILED: {str(e)}")
            return {
                "status": "failed",
                "error": str(e),
                "message": f"Pipeline failed: {str(e)}",
            }

    async def cleanup(self) -> None:
        self.tmp.cleanup()
        logger.debug("cleaned up temp files")


def parsed_tags(tag_list):
    tags = {}
    internal_tag_keys = {"bench_src", "cand_src", "scenario", "catchment"}
    required_tag_keys = {"batch_name", "aoi_name"}

    forbidden_chars = {" ", "/", "&", ","}

    for tag in tag_list:
        if "=" not in tag:
            raise argparse.ArgumentTypeError(f"Invalid tag format: '{tag}'. Expected key=value.")
        key, value = tag.split("=", 1)

        for char in forbidden_chars:
            if char in key:
                raise argparse.ArgumentTypeError(
                    f"Tag key '{key}' contains forbidden character '{char}'. Forbidden characters: {', '.join(sorted(forbidden_chars))}"
                )

        for char in forbidden_chars:
            if char in value:
                raise argparse.ArgumentTypeError(
                    f"Tag value '{value}' contains forbidden character '{char}'. Forbidden characters: {', '.join(sorted(forbidden_chars))}"
                )

        tags[key] = value

    for internal_key in internal_tag_keys:
        if internal_key in tags:
            raise argparse.ArgumentTypeError(
                f"Tag '{internal_key}' is reserved for internal use and cannot be provided by users."
            )

    for required_key in required_tag_keys:
        if required_key not in tags:
            raise argparse.ArgumentTypeError(
                f"Required tag '{required_key}' is missing. Required tags: {', '.join(sorted(required_tag_keys))}"
            )

    if tags:
        tags_str = ",".join(f"{k}={v}" for k, v in tags.items())
        if len(tags_str) > 120:
            raise argparse.ArgumentTypeError(f"Tags exceed 120 character limit ({len(tags_str)} chars): {tags_str}")

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

    temp_log_file = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix="_pipeline.log")
    temp_log_path = temp_log_file.name
    temp_log_file.close()

    file_handler = logging.FileHandler(temp_log_path)
    file_handler.setLevel(os.environ.get("LOG_LEVEL", "INFO"))
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))

    root_logger = logging.getLogger()
    root_logger.addHandler(file_handler)

    cfg = load_config()

    async def _main():
        # Validate AOI file extension
        if not args.aoi.lower().endswith(".gpkg"):
            raise ValueError(f"AOI file must be a GPKG file, got: {args.aoi}")

        outputs_path = args.outputs_path

        if os.path.exists(outputs_path):
            raise ValueError(f"Output directory already exists: {outputs_path}")

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

            logging.info(f"Using HAND index path: {args.hand_index_path}")

            pipeline = PolygonPipeline(cfg, nomad, data_svc, polygon_gdf, args.tags, outputs_path, log_db)
            logging.info(f"Started pipeline run for {args.aoi} with outputs to {outputs_path}")

            try:
                summary = await pipeline.run()

                logger.info("=" * 80)
                logger.info("PIPELINE FINAL RESULTS:")
                logger.info(json.dumps(summary, indent=2))
                logger.info("=" * 80)

                file_handler.close()
                root_logger.removeHandler(file_handler)

                final_log_path = pipeline.path_factory.logs_path()
                await data_svc.copy_file_to_uri(temp_log_path, final_log_path)
                logging.info(f"Logs written to {final_log_path}")

                print(json.dumps(summary, indent=2))
            finally:
                if os.path.exists(temp_log_path):
                    os.unlink(temp_log_path)

                await pipeline.cleanup()
                await nomad.stop()
                await log_db.close()
                data_svc.cleanup()

    asyncio.run(_main())
