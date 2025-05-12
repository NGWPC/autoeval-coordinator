import asyncio
import logging
from typing import Any, Dict, List, Optional

import smart_open
from pystac_client import Client
import pandas as pd
import duckdb
from load_config import AppConfig
from stac_utils import format_results, merge_gfm_expanded, dictify
from hand_catchments import get_catchment_data_for_polygon, filter_dataframes_by_overlap


class DataService:
    """
    - If use_mock_data=True: load a directory of Parquet table of mock attributes,
      group by catchment_id, write per‐catchment Parquet to S3.
    - Else: connect to DuckDB, run real spatial queries, write per‐catchment Parquet to S3.
    - query a STAC API and group assets into {collection_short → flow_scenario → {extents, flowfiles}}
    """

    def __init__(self, config: AppConfig, db_path: Optional[str] = None):
        self.config = config
        self.use_mock = config.use_mock_data
        # override or use config.db.path
        self.db_path = db_path or (config.db.path if config.db else None)
        if not self.use_mock and not self.db_path:
            raise ValueError("DuckDB path must be provided in real mode")

        # cache for mock table
        self._cached_df: Optional[pd.DataFrame] = None

        # lazy‐init for the STAC client
        self._stac_client: Optional[Client] = None

        # pass any S3 transport options into pandas.to_parquet
        self._transport_params = config.s3.transport_params or {}

    async def query_for_catchments(
        self, polygon_data: Dict[str, Any], pipeline_id: str
    ) -> Dict[str, Any]:
        """
        Returns a dict:
          {
            "catchments": {
              catchment_id: {"catchment_data_path": "s3://.../catchment_data.parquet"},
              ...
            }
          }
        """
        if self.use_mock:
            return await self._get_and_write_mock_catchments(polygon_data, pipeline_id)
        else:
            return await self._get_and_write_real_catchments(polygon_data, pipeline_id)

    async def _get_and_write_mock_catchments(
        self, polygon_data: Dict[str, Any], pipeline_id: str
    ) -> Dict[str, Any]:
        import os

        # lazy load
        if self._cached_df is None:
            path = self.config.mock_data_paths.mock_catchment_data
            logging.info(f"Loading mock catchment Parquet from: {path}")
            self._cached_df = pd.read_parquet(path)

        df = self._cached_df
        bucket = self.config.s3.bucket
        prefix = self.config.s3.base_prefix

        catchments: Dict[str, Dict[str, str]] = {}

        # group by catchment_id
        for catch_id, group in df.groupby("catchment_id"):
            base = f"s3://{bucket}/{prefix}/pipeline_{pipeline_id}/catchment_{catch_id}"
            uri = f"{base}/catchment_data.parquet"

            # drop the id column inside
            df_to_write = group.drop(columns=["catchment_id"])

            # write
            await self.write_parquet_to_uri(df_to_write, uri)
            logging.info("Wrote mock catchment %s → %s", catch_id, uri)

            catchments[str(catch_id)] = {"catchment_data_path": uri}

        return {"catchments": catchments}

    async def _get_and_write_real_catchments(
        self, polygon_data: Dict[str, Any], pipeline_id: str
    ) -> Dict[str, Any]:
        bucket = self.config.s3.bucket
        prefix = self.config.s3.base_prefix

        # open DuckDB
        con = duckdb.connect(database=self.db_path, read_only=True)
        try:
            con.execute("LOAD spatial;")
        except duckdb.Error:
            logging.warning("Could not load DuckDB 'spatial' extension.")

        # run the two‐step query
        geoms_gdf, attrs_df, query_poly = get_catchment_data_for_polygon(
            polygon_data, con
        )
        con.close()

        if attrs_df.empty:
            logging.info("Real query yielded no catchments.")
            return {"catchments": {}}

        # apply overlap‐filter
        # read from config.defaults.catchment_overlap_percent if available
        threshold = getattr(self.config.defaults, "catchment_overlap_percent", 10.0)
        fg, fa, stats = filter_dataframes_by_overlap(
            geoms_gdf, attrs_df, query_poly, threshold
        )
        logging.info("Overlap filter summary: %s", stats)

        if fa.empty:
            logging.info("All catchments removed by overlap filter.")
            return {"catchments": {}}

        # write one Parquet per remaining catchment
        catchments: Dict[str, Dict[str, str]] = {}
        for catch_id, group in fa.groupby("catchment_id"):
            base = f"s3://{bucket}/{prefix}/pipeline_{pipeline_id}/catchment_{catch_id}"
            uri = f"{base}/catchment_data.parquet"
            df_to_write = group.drop(columns=["catchment_id"])

            await self.write_parquet_to_uri(df_to_write, uri)
            logging.info("Wrote real catchment %s → %s", catch_id, uri)

            catchments[str(catch_id)] = {"catchment_data_path": uri}

        return {"catchments": catchments}

    async def write_parquet_to_uri(self, df: pd.DataFrame, uri: str):
        """Async wrapper; uses pandas.to_parquet under the hood."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._sync_write_parquet, df, uri)

    def _sync_write_parquet(self, df, uri: str):
        """
        Synchronous helper for pandas.to_parquet.
        If s3://, pass transport_params to storage_options.
        """
        if uri.startswith("s3://"):
            df.to_parquet(uri, index=False, storage_options=self._transport_params)
        else:
            df.to_parquet(uri, index=False)

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
