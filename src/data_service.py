import asyncio
import json
import logging
from typing import Any, Dict, List, Optional

import smart_open
from pystac_client import Client

from load_config import AppConfig
from stac_utils import format_results, merge_gfm_expanded, dictify


class DataService:
    """
    Service to:
      • return catchment data (for inundator jobs)
      • write JSON to S3/local URIs
      • query a STAC API and group assets into {collection_short → flow_scenario → {extents, flowfiles}}
    """

    def __init__(self, config: AppConfig):
        self.config = config
        # for your mock catchment data
        self.mock_data_path = config.mock_data_paths.mock_catchment_data
        self._cached_data: Optional[Dict[str, Any]] = None
        # pass any S3 transport params (e.g. region) into smart_open
        self._transport_params = config.s3.transport_params or {}
        # lazy‐init for the STAC client
        self._stac_client: Optional[Client] = None

    # Mock‐catchment I/O

    async def _load_mock_data(self) -> Dict[str, Any]:
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
            logging.error(f"Could not load mock catchment data: {e}")
            return {"catchments": {}, "hand_version": "error"}

    async def query_for_catchments(self, polygon_data: Dict[str, Any]) -> Dict:
        """
        Returns catchment data for the given polygon.
        In this mock implementation, we return the same data regardless of polygon_data.

        Args:
            polygon_data: Dictionary containing polygon information

        Returns:
            Dictionary with catchments and hand_version
        """
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

    # STAC Querying

    @property
    def stac_client(self) -> Client:
        """Lazily open the STAC API via pystac-client."""
        if self._stac_client is None:
            self._stac_client = Client.open(self.config.stac.api_url)
            logging.info(f"Connected to STAC API at {self.config.stac.api_url}")
        return self._stac_client

    async def query_stac_groups(
        self,
        datetime: Optional[str] = None,
        intersects: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Dict[str, Dict[str, List[str]]]]:
        """
        Async wrapper around the sync STAC search → grouping logic.
        Returns a dict:
          { collection_short_name → group_id → { "extents": [...], "flowfiles": [...] } }
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            self._sync_query_and_group,
            datetime,
            intersects,
        )

    def _sync_query_and_group(
        self,
        datetime: Optional[str],
        intersects: Optional[Dict[str, Any]],
    ) -> Dict[str, Dict[str, Dict[str, List[str]]]]:
        # build search arguments
        search_kwargs: Dict[str, Any] = {"collections": self.config.stac.collections}
        if datetime:
            search_kwargs["datetime"] = datetime
        if intersects:
            search_kwargs["intersects"] = intersects

        logging.info(f"Searching STAC with {search_kwargs}")
        search = self.stac_client.search(**search_kwargs)

        # get all matching items
        items = list(search.get_all_items())
        logging.info(f"Fetched {len(items)} items from STAC")

        # apply your grouping rules
        grouped = format_results(items)

        # merge GFM‐expanded intervals:
        if "gfm_expanded" in grouped:
            grouped["gfm_expanded"] = merge_gfm_expanded(
                grouped["gfm_expanded"], tolerance_days=3
            )
        return dictify(grouped)
