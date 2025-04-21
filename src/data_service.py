import asyncio
import json
import logging
import os
from typing import Dict, Any
from contextlib import suppress
import smart_open  # Import smart_open

from load_config import AppConfig


class DataService:
    """Service to query data sources and interact with S3 via smart_open."""

    def __init__(self, config: AppConfig):
        self.config = config
        self.mock_data_path = config.mock_data_paths.mock_catchment_data
        self._cached_data = None
        # Optional: configure transport params for smart_open if needed
        self._transport_params = None  # E.g. config.s3.transport_params

    async def _load_mock_data(self) -> Dict:
        if self._cached_data:
            return self._cached_data
        logging.info(f"Loading mock catchment data from: {self.mock_data_path}")
        try:
            # Use smart_open for reading local file too (consistency)
            with smart_open.open(self.mock_data_path, "r") as f:
                data = json.load(f)
            self._cached_data = data
            return data
        except FileNotFoundError:
            logging.error(f"Mock data file not found: {self.mock_data_path}")
            return {"catchments": {}, "hand_version": "error_no_file"}
        except Exception as e:
            logging.error(f"Error reading/parsing {self.mock_data_path}: {e}")
            return {"catchments": {}, "hand_version": "error_read_decode"}

    async def query_for_catchments(self, polygon_data: Dict[str, Any]) -> Dict:
        """
        Returns catchment data for the given polygon.
        In this mock implementation, we return the same data regardless of polygon_data.

        Args:
            polygon_data: Dictionary containing polygon information

        Returns:
            Dictionary with catchments and hand_version
        """
        # Simulate a brief delay as if we're querying a service
        await asyncio.sleep(0.01)

        # Log the polygon data for informational purposes
        polygon_id = polygon_data.get("polygon_id", "unknown")
        logging.info(f"Querying catchments for polygon: {polygon_id}")

        # Load the mock data
        data = await self._load_mock_data()

        logging.info(
            f"Data service returning {len(data.get('catchments', {}))} catchments for polygon {polygon_id}."
        )
        return data

    async def write_json_to_uri(self, data: Dict, uri: str):
        """Writes dictionary as JSON to a URI (local or S3) using smart_open."""
        logging.debug(f"Writing JSON data to: {uri}")
        try:
            # smart_open write is synchronous, run in executor
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,  # Use default thread pool executor
                self._sync_write_json,
                data,
                uri,
            )
            logging.info(f"Successfully wrote JSON to {uri}")
        except Exception as e:
            logging.exception(f"Failed to write JSON to {uri}")
            raise ConnectionError(f"Failed to write JSON to {uri}") from e

    def _sync_write_json(self, data: Dict, uri: str):
        """Synchronous helper for writing JSON using smart_open."""
        transport_params = self._transport_params
        with smart_open.open(uri, "w", transport_params=transport_params) as f:
            json.dump(data, f, indent=2)
