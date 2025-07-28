import asyncio
import json
import logging
import os
import shutil
import tempfile
from contextlib import suppress
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3
import fsspec
import geopandas as gpd
from botocore.exceptions import ClientError, NoCredentialsError
from shapely.geometry import shape

from flowfile_combiner import FlowfileCombiner
from hand_index_querier import HandIndexQuerier
from load_config import AppConfig
from stac_querier import StacQuerier


class DataService:
    """Service to query data sources and interact with S3 via fsspec."""

    def __init__(self, config: AppConfig, hand_index_path: str, benchmark_collections: Optional[List[str]] = None):
        self.config = config
        
        # Configure S3 filesystem options - try config first, then IAM credentials
        self._s3_options = {}
        if config.aws.AWS_ACCESS_KEY_ID:
            self._s3_options["key"] = config.aws.AWS_ACCESS_KEY_ID
        if config.aws.AWS_SECRET_ACCESS_KEY:
            self._s3_options["secret"] = config.aws.AWS_SECRET_ACCESS_KEY
        if config.aws.AWS_SESSION_TOKEN:
            self._s3_options["token"] = config.aws.AWS_SESSION_TOKEN
            
        # If no explicit credentials in config, try to get from IAM instance profile
        if not self._s3_options:
            try:
                credentials = boto3.Session().get_credentials()
                if credentials:
                    self._s3_options["key"] = credentials.access_key
                    self._s3_options["secret"] = credentials.secret_key
                    if credentials.token:
                        self._s3_options["token"] = credentials.token
                    logging.info("Using IAM instance profile credentials for S3 access")
                else:
                    logging.warning("No AWS credentials found in config or IAM instance profile")
            except Exception as e:
                logging.warning(f"Failed to get IAM credentials: {e}")

        # Initialize HandIndexQuerier with provided path
        self.hand_querier = None
        if hand_index_path:
            self.hand_querier = HandIndexQuerier(
                partitioned_base_path=hand_index_path,
                overlap_threshold_percent=config.hand_index.overlap_threshold_percent,
            )

        # Initialize StacQuerier
        self.stac_querier = None
        if config.stac:
            self.stac_querier = StacQuerier(
                api_url=config.stac.api_url,
                collections=benchmark_collections,
                overlap_threshold_percent=config.stac.overlap_threshold_percent,
                datetime_filter=config.stac.datetime_filter,
            )

        # Initialize FlowfileCombiner
        self.flowfile_combiner = FlowfileCombiner(
            output_dir=config.flow_scenarios.output_dir if config.flow_scenarios else "combined_flowfiles"
        )

    def load_polygon_gdf_from_file(self, file_path: str) -> gpd.GeoDataFrame:
        """
        Args:
            file_path: Path to the GeoDataFrame file (local or S3)

        Returns:
            GeoDataFrame with polygon geometry

        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If file is empty or invalid
        """
        try:
            # geopandas can read from both local and S3 paths
            # For S3 paths, it will use storage_options; for local paths, it ignores them
            gdf = gpd.read_file(file_path, storage_options=self._s3_options if file_path.startswith("s3://") else None)

            if len(gdf) == 0:
                raise ValueError(f"Empty GeoDataFrame in file: {file_path}")

            # Ensure CRS is EPSG:4326
            if gdf.crs and gdf.crs.to_epsg() != 4326:
                logging.info(f"Converting polygon data from {gdf.crs} to EPSG:4326")
                gdf = gdf.to_crs("EPSG:4326")
            elif not gdf.crs:
                logging.warning(f"No CRS found in {file_path}, assuming EPSG:4326")
                gdf.set_crs("EPSG:4326", inplace=True)

            logging.info(f"Loaded polygon GeoDataFrame with {len(gdf)} features from {file_path}")
            return gdf

        except FileNotFoundError:
            raise FileNotFoundError(f"Polygon data file not found: {file_path}")
        except Exception as e:
            raise ValueError(f"Error loading GeoDataFrame from {file_path}: {e}")

    async def query_stac_for_flow_scenarios(self, polygon_gdf: gpd.GeoDataFrame) -> Dict:
        """
        Query STAC API for flow scenarios based on polygon.

        Args:
            polygon_gdf: GeoDataFrame containing polygon geometry

        Returns:
            Dictionary with STAC query results and combined flowfiles
        """
        # Generate polygon_id from index or use a default
        polygon_id = f"polygon_{len(polygon_gdf)}" if len(polygon_gdf) > 0 else "unknown"

        if not self.stac_querier or not self.config.stac:
            raise RuntimeError("STAC configuration is required but not provided")

        logging.info(f"Querying STAC for flow scenarios for polygon: {polygon_id}")

        try:
            # Run STAC query in executor to avoid blocking
            loop = asyncio.get_running_loop()
            stac_results = await loop.run_in_executor(
                None,
                self.stac_querier.query_stac_for_polygon,
                polygon_gdf,
                None,  # roi_geojson not needed since we have polygon_gdf
            )

            if not stac_results:
                logging.info(f"No STAC results found for polygon {polygon_id}")
                return {"scenarios": {}}

            # Process flowfiles from STAC results
            combined_flowfiles = {}
            if self.flowfile_combiner:
                logging.info(f"Processing flowfiles for {len(stac_results)} collections")

                # Create temporary directory for this polygon's combined flowfiles
                temp_dir = f"/tmp/flow_scenarios_{polygon_id}"
                Path(temp_dir).mkdir(parents=True, exist_ok=True)

                combined_flowfiles = await loop.run_in_executor(
                    None,
                    self.flowfile_combiner.process_stac_query_results,
                    stac_results,
                    temp_dir,
                )

                logging.info(f"Combined flowfiles created for polygon {polygon_id}")

            return {
                "scenarios": stac_results,
                "combined_flowfiles": combined_flowfiles,
                "polygon_id": polygon_id,
            }

        except Exception as e:
            logging.error(f"Error in STAC query for polygon {polygon_id}: {e}")
            return {"scenarios": {}, "error": str(e)}

    async def query_for_catchments(self, polygon_gdf: gpd.GeoDataFrame) -> Dict:
        """
        Returns catchment data for the given polygon.

        Args:
            polygon_gdf: GeoDataFrame containing polygon geometry

        Returns:
            Dictionary with catchments mapping IDs to parquet file paths
        """
        # Simulate a brief delay as if we're querying a service
        await asyncio.sleep(0.01)

        # Generate polygon_id from index or use a default
        polygon_id = f"polygon_{len(polygon_gdf)}" if len(polygon_gdf) > 0 else "unknown"
        logging.info(f"Querying catchments for polygon: {polygon_id}")

        if self.hand_querier:
            # Use real hand index query
            try:
                # Create temporary directory for parquet outputs
                with tempfile.TemporaryDirectory(prefix=f"catchments_{polygon_id}_") as temp_dir:
                    # Run the query with temporary output directory
                    loop = asyncio.get_running_loop()
                    catchments_result = await loop.run_in_executor(
                        None,
                        self.hand_querier.query_catchments_for_polygon,
                        polygon_gdf,
                        temp_dir,
                    )

                    if not catchments_result:
                        logging.info(f"No catchments found for polygon {polygon_id}")
                        return {"catchments": {}, "hand_version": "real_query"}

                    # Copy parquet files to a more permanent location if needed
                    # For now, we'll keep them in temp and rely on the pipeline to copy to S3
                    catchments = {}
                    for catch_id, info in catchments_result.items():
                        # The parquet files are in temp_dir, we need to ensure they persist
                        # Copy to a location that won't be cleaned up immediately
                        temp_file = Path(info["parquet_path"])
                        persistent_dir = Path(f"/tmp/catchments_{polygon_id}")
                        persistent_dir.mkdir(exist_ok=True)
                        persistent_path = persistent_dir / temp_file.name

                        shutil.copy2(temp_file, persistent_path)

                        catchments[catch_id] = {
                            "parquet_path": str(persistent_path),
                            "row_count": info.get("row_count", 0),
                        }

                    logging.info(
                        f"Data service returning {len(catchments)} catchments for polygon {polygon_id} (real query)."
                    )
                    return {"catchments": catchments, "hand_version": "real_query"}

            except Exception as e:
                logging.error(f"Error in hand index query for polygon {polygon_id}: {e}")
                raise

        # No hand querier available
        raise RuntimeError(f"Hand index querier is required but not initialized for polygon {polygon_id}")

    async def copy_file_to_uri(self, source_path: str, dest_uri: str):
        """Copies a file (e.g., parquet) to a URI (local or S3) using fsspec.
        Copies if source is local and destination is S3, or if both are local but paths differ."""

        # Normalize paths for comparison
        source_path_normalized = (
            os.path.abspath(source_path)
            if not source_path.startswith(("s3://", "http://", "https://"))
            else source_path
        )
        dest_uri_normalized = (
            os.path.abspath(dest_uri) if not dest_uri.startswith(("s3://", "http://", "https://")) else dest_uri
        )

        # Copy if source is local and destination is S3, or if both are local but paths differ
        should_copy = (
            not source_path.startswith(("s3://", "http://", "https://")) and dest_uri.startswith("s3://")
        ) or (
            not source_path.startswith(("s3://", "http://", "https://"))
            and not dest_uri.startswith(("s3://", "http://", "https://"))
            and source_path_normalized != dest_uri_normalized
        )

        if should_copy:
            logging.debug(f"Copying file from {source_path} to {dest_uri}")
            try:
                if not dest_uri.startswith(("s3://", "http://", "https://")):
                    os.makedirs(os.path.dirname(dest_uri), exist_ok=True)

                # Run in executor for async operation
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(
                    None,
                    self._sync_copy_file,
                    source_path,
                    dest_uri,
                )
                logging.info(f"Successfully copied {source_path} to {dest_uri}")
                return dest_uri
            except Exception as e:
                logging.exception(f"Failed to copy {source_path} to {dest_uri}")
                raise ConnectionError(f"Failed to copy file to {dest_uri}") from e
        else:
            # If already on S3 or both local with same path, just return the source path
            logging.debug(f"No copy needed - using existing path: {source_path}")
            return source_path

    def _sync_copy_file(self, source_path: str, dest_uri: str):
        """Synchronous helper for copying files using fsspec."""
        # Copy file using fsspec.open directly (allows fallback to default AWS credentials)
        with open(source_path, "rb") as src:
            with fsspec.open(
                dest_uri, "wb", **self._s3_options if dest_uri.startswith("s3://") else {}
            ) as dst:
                dst.write(src.read())

    async def check_file_exists(self, uri: str) -> bool:
        """Check if a file exists (S3 or local).

        Args:
            uri: S3 URI or local path to check

        Returns:
            True if file exists, False otherwise
        """
        try:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                None,
                self._sync_check_file_exists,
                uri,
            )
        except Exception as e:
            logging.warning(f"Error checking file {uri}: {e}")
            return False

    def _sync_check_file_exists(self, uri: str) -> bool:
        """Synchronous helper to check if file exists (S3 or local)."""
        try:
            if uri.startswith("s3://"):
                fs = fsspec.filesystem("s3", **self._s3_options)
                return fs.exists(uri)
            else:
                # Local file
                return Path(uri).exists()
        except (FileNotFoundError, NoCredentialsError, ClientError) as e:
            logging.debug(f"File {uri} does not exist or is not accessible: {e}")
            return False
        except Exception as e:
            logging.warning(f"Unexpected error checking file {uri}: {e}")
            return False

    async def validate_files(self, uris: List[str]) -> List[str]:
        """Validate a list of S3 URIs or local file paths and return only the ones that exist.

        Args:
            uris: List of S3 URIs or local file paths to validate

        Returns:
            List of URIs/paths that exist and are accessible
        """
        if not uris:
            return []

        logging.info(f"Validating {len(uris)} files...")

        # Check all files concurrently
        tasks = [self.check_file_exists(uri) for uri in uris]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        valid_uris = []
        missing_uris = []

        for uri, result in zip(uris, results):
            if isinstance(result, Exception):
                logging.error(f"Error validating file {uri}: {result}")
                missing_uris.append(uri)
            elif result:
                valid_uris.append(uri)
            else:
                missing_uris.append(uri)

        if missing_uris:
            logging.info(f"Found {len(missing_uris)} missing files:")
            for uri in missing_uris:
                logging.info(f"  Missing: {uri}")

        logging.info(f"Validated files: {len(valid_uris)} exist, {len(missing_uris)} missing")
        return valid_uris

    def find_metrics_files(self, base_path: str) -> List[str]:
        """Find all metrics.csv files in the output directory structure.

        Args:
            base_path: Base output path (local or S3)

        Returns:
            List of paths to metrics.csv files
        """
        metrics_files = []

        try:
            if base_path.startswith("s3://"):
                fs = fsspec.filesystem("s3", **self._s3_options)
                # Use glob to find all metrics.csv files recursively (including tagged versions)
                pattern = f"{base_path.rstrip('/')}/**/*__metrics.csv"
                metrics_files = fs.glob(pattern)
                # Add s3:// prefix back since glob strips it
                metrics_files = [f"s3://{path}" for path in metrics_files]
            else:
                base_path_obj = Path(base_path)
                if base_path_obj.exists():
                    metrics_files = [str(p) for p in base_path_obj.glob("**/*__metrics.csv")]

            logging.info(f"Found {len(metrics_files)} metrics.csv files in {base_path}")
            return metrics_files

        except Exception as e:
            logging.error(f"Error finding metrics files in {base_path}: {e}")
            return []

    def cleanup(self):
        """Clean up resources, including HandIndexQuerier connection."""
        if self.hand_querier:
            self.hand_querier.close()
            self.hand_querier = None
