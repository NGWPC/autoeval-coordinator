import asyncio
import json
import logging
import os
from contextlib import suppress
from pathlib import Path
from typing import Any, Dict, List

import smart_open  # Import smart_open
from botocore.exceptions import NoCredentialsError, ClientError

from load_config import AppConfig


class DataService:
    """Service to query data sources and interact with S3 via smart_open."""

    def __init__(self, config: AppConfig):
        self.config = config
        self.mock_data_path = config.mock_data_paths.mock_catchment_data
        self._cached_parquet_files = None
        # Optional: configure transport params for smart_open if needed
        self._transport_params = None  # E.g. config.s3.transport_params

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

    async def query_for_catchments(self, polygon_data: Dict[str, Any]) -> Dict:
        """
        Returns catchment data for the given polygon.
        In this mock implementation, we return parquet file paths as catchment IDs.

        Args:
            polygon_data: Dictionary containing polygon information

        Returns:
            Dictionary with catchments mapping IDs to parquet file paths
        """
        # Simulate a brief delay as if we're querying a service
        await asyncio.sleep(0.01)

        # Log the polygon data for informational purposes
        polygon_id = polygon_data.get("polygon_id", "unknown")
        logging.info(f"Querying catchments for polygon: {polygon_id}")

        # Load the parquet file list
        parquet_files = await self._load_parquet_file_list()

        # Create catchments dict with catchment ID as key and parquet path as value
        # Extract catchment ID from filename (UUID before .parquet)
        catchments = {}
        for pf in parquet_files:
            filename = Path(pf).stem  # Get filename without extension
            catchments[filename] = {"parquet_path": pf}

        logging.info(f"Data service returning {len(catchments)} catchments for polygon {polygon_id}.")
        return {"catchments": catchments, "hand_version": "parquet_based"}

    async def copy_file_to_uri(self, source_path: str, dest_uri: str):
        """Copies a file (e.g., parquet) to a URI (local or S3) using smart_open.
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
        """Synchronous helper for copying files using smart_open."""
        transport_params = self._transport_params
        with open(source_path, "rb") as src:
            with smart_open.open(dest_uri, "wb", transport_params=transport_params) as dst:
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
            with smart_open.open(s3_uri, "rb", transport_params=self._transport_params) as f:
                # Try to read a single byte to confirm file exists and is readable
                f.read(1)
            return True
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
