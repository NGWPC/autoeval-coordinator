import asyncio
import json
import logging
import os
import tempfile
from contextlib import suppress
from pathlib import Path
from typing import Any, Dict, List

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

    def __init__(self, config: AppConfig):
        self.config = config
        self.mock_data_path = config.mock_data_paths.mock_catchment_data
        self._cached_parquet_files = None
        # Configure S3 filesystem options from environment
        self._s3_options = {}
        if config.s3.AWS_ACCESS_KEY_ID:
            self._s3_options["key"] = config.s3.AWS_ACCESS_KEY_ID
        if config.s3.AWS_SECRET_ACCESS_KEY:
            self._s3_options["secret"] = config.s3.AWS_SECRET_ACCESS_KEY
        if config.s3.AWS_SESSION_TOKEN:
            self._s3_options["token"] = config.s3.AWS_SESSION_TOKEN

        # Initialize HandIndexQuerier if enabled
        self.hand_querier = None
        if config.hand_index.enabled:
            self.hand_querier = HandIndexQuerier(
                partitioned_base_path=config.hand_index.partitioned_base_path,
                overlap_threshold_percent=config.hand_index.overlap_threshold_percent,
            )

        # Initialize StacQuerier if enabled
        self.stac_querier = None
        if config.stac and config.stac.enabled:
            self.stac_querier = StacQuerier(
                api_url=config.stac.api_url,
                collections=config.stac.collections,
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
            file_path: Path to the GeoDataFrame file

        Returns:
            GeoDataFrame with polygon geometry

        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If file is empty or invalid
        """
        if not Path(file_path).exists():
            raise FileNotFoundError(f"Polygon data file not found: {file_path}")

        try:
            gdf = gpd.read_file(file_path)

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

        except Exception as e:
            raise ValueError(f"Error loading GeoDataFrame from {file_path}: {e}")

    async def _load_parquet_file_list(self) -> List[str]:
        """Load list of parquet files from the mock data directory."""
        if self._cached_parquet_files:
            return self._cached_parquet_files

        logging.info(f"Loading parquet files from: {self.mock_data_path}")
        try:
            # Get all parquet files in the directory
            data_dir = Path(self.mock_data_path)
            if not data_dir.exists():
                raise FileNotFoundError(f"Directory not found: {self.mock_data_path}")

            parquet_files = list(data_dir.glob("*.parquet"))
            if not parquet_files:
                raise ValueError(f"No parquet files found in {self.mock_data_path}")

            self._cached_parquet_files = [str(f) for f in parquet_files]
            logging.info(f"Found {len(self._cached_parquet_files)} parquet files")
            return self._cached_parquet_files
        except Exception as e:
            logging.error(f"Error loading parquet files from {self.mock_data_path}: {e}")
            return []

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

        # Check if we should use mock STAC data (only if STAC is disabled)
        if (
            (not self.stac_querier or not self.config.stac or not self.config.stac.enabled)
            and self.config.mock_data_paths.mock_stac_results
            and Path(self.config.mock_data_paths.mock_stac_results).exists()
        ):
            logging.info(f"Using mock STAC results from {self.config.mock_data_paths.mock_stac_results}")
            try:
                with open(self.config.mock_data_paths.mock_stac_results, "r") as f:
                    stac_results = json.load(f)

                # Process flowfiles from STAC results
                combined_flowfiles = {}
                if self.flowfile_combiner:
                    logging.info(f"Processing flowfiles for {len(stac_results)} collections")

                    # Create temporary directory for this polygon's combined flowfiles
                    temp_dir = f"/tmp/flow_scenarios_{polygon_id}"
                    Path(temp_dir).mkdir(parents=True, exist_ok=True)

                    loop = asyncio.get_running_loop()
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
                    "stac_enabled": False,  # Indicate we used mock data
                    "polygon_id": polygon_id,
                    "mock_data": True,
                }
            except Exception as e:
                logging.error(f"Error loading mock STAC results: {e}")
                # Fall through to real STAC query or disabled state

        if not self.stac_querier or not self.config.stac or not self.config.stac.enabled:
            logging.info("STAC querying is disabled")
            return {"scenarios": {}, "stac_enabled": False}

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
                return {"scenarios": {}, "stac_enabled": True}

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
                "stac_enabled": True,
                "polygon_id": polygon_id,
            }

        except Exception as e:
            logging.error(f"Error in STAC query for polygon {polygon_id}: {e}")
            return {"scenarios": {}, "stac_enabled": True, "error": str(e)}

    async def query_for_catchments(self, polygon_gdf: gpd.GeoDataFrame) -> Dict:
        """
        Returns catchment data for the given polygon.
        Uses HandIndexQuerier for real spatial queries if enabled, otherwise uses mock data.

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

        if self.hand_querier and self.config.hand_index.enabled:
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

                        # Copy the file to persistent location
                        import shutil

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
                logging.info("Falling back to mock data")
                # Fall through to mock data logic

        # Use mock data (original logic)
        parquet_files = await self._load_parquet_file_list()

        # Create catchments dict with catchment ID as key and parquet path as value
        # Extract catchment ID from filename (UUID before .parquet)
        catchments = {}
        for pf in parquet_files:
            filename = Path(pf).stem  # Get filename without extension
            catchments[filename] = {"parquet_path": pf}

        logging.info(f"Data service returning {len(catchments)} catchments for polygon {polygon_id} (mock data).")
        return {"catchments": catchments, "hand_version": "mock_data"}

    async def copy_file_to_uri(self, source_path: str, dest_uri: str):
        """Copies a file (e.g., parquet) to a URI (local or S3) using fsspec.
        Only copies if source is local and destination is S3."""

        # Only copy if source is local and destination is S3
        if not source_path.startswith(("s3://", "http://", "https://")) and dest_uri.startswith("s3://"):
            logging.debug(f"Copying file from {source_path} to {dest_uri}")
            try:
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
            # If already on S3 or both local, just return the source path
            logging.debug(f"No copy needed - using existing path: {source_path}")
            return source_path

    def _sync_copy_file(self, source_path: str, dest_uri: str):
        """Synchronous helper for copying files using fsspec."""
        # Create filesystem based on destination URI
        if dest_uri.startswith("s3://"):
            fs = fsspec.filesystem("s3", **self._s3_options)
        else:
            fs = fsspec.filesystem("file")

        # Copy file using fsspec
        with open(source_path, "rb") as src:
            with fs.open(dest_uri, "wb") as dst:
                dst.write(src.read())

    async def check_s3_file_exists(self, s3_uri: str) -> bool:
        """Check if an S3 file exists.

        Args:
            s3_uri: S3 URI to check (e.g., "s3://bucket/path/file.tif")

        Returns:
            True if file exists, False otherwise
        """
        try:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                None,
                self._sync_check_s3_file_exists,
                s3_uri,
            )
        except Exception as e:
            logging.warning(f"Error checking S3 file {s3_uri}: {e}")
            return False

    def _sync_check_s3_file_exists(self, s3_uri: str) -> bool:
        """Synchronous helper to check if S3 file exists."""
        try:
            fs = fsspec.filesystem("s3", **self._s3_options)
            return fs.exists(s3_uri)
        except (FileNotFoundError, NoCredentialsError, ClientError) as e:
            logging.debug(f"S3 file {s3_uri} does not exist or is not accessible: {e}")
            return False
        except Exception as e:
            logging.warning(f"Unexpected error checking S3 file {s3_uri}: {e}")
            return False

    async def validate_s3_files(self, s3_uris: List[str]) -> List[str]:
        """Validate a list of S3 URIs and return only the ones that exist.

        Args:
            s3_uris: List of S3 URIs to validate

        Returns:
            List of S3 URIs that exist and are accessible
        """
        if not s3_uris:
            return []

        logging.info(f"Validating {len(s3_uris)} S3 files...")

        # Check all files concurrently
        tasks = [self.check_s3_file_exists(uri) for uri in s3_uris]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        valid_uris = []
        missing_uris = []

        for uri, result in zip(s3_uris, results):
            if isinstance(result, Exception):
                logging.error(f"Error validating S3 file {uri}: {result}")
                missing_uris.append(uri)
            elif result:
                valid_uris.append(uri)
            else:
                missing_uris.append(uri)

        if missing_uris:
            logging.info(f"Found {len(missing_uris)} missing S3 files:")
            for uri in missing_uris:
                logging.info(f"  Missing: {uri}")

        logging.info(f"Validated S3 files: {len(valid_uris)} exist, {len(missing_uris)} missing")
        return valid_uris

    def cleanup(self):
        """Clean up resources, including HandIndexQuerier connection."""
        if self.hand_querier:
            self.hand_querier.close()
            self.hand_querier = None
