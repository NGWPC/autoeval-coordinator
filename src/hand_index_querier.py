import logging
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import boto3
import duckdb
import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon
from shapely.wkb import loads
from shapely.wkt import dumps

logger = logging.getLogger(__name__)


class HandIndexQuerier:
    """
    A class for querying HAND index data from parquet files.
    Provides spatial intersection and filtering capabilities.
    """

    def __init__(self, partitioned_base_path: str, overlap_threshold_percent: float = 10.0):
        """
        Initialize the HandIndexQuerier.

        Args:
            partitioned_base_path: Base path to parquet files (local or s3://)
            overlap_threshold_percent: Minimum overlap percentage to keep a catchment
        """
        self.partitioned_base_path = partitioned_base_path
        self.overlap_threshold_percent = overlap_threshold_percent
        self.con = None
        self.credentials = boto3.Session().get_credentials()
        self.s3_region = boto3.Session.region_name

    def _ensure_connection(self):
        """Ensure DuckDB connection is established with required extensions."""
        if self.con is None:
            self.con = duckdb.connect(":memory:")

            # Load required extensions
            try:
                self.con.execute("INSTALL spatial;")
                self.con.execute("LOAD spatial;")
                self.con.execute("INSTALL httpfs;")
                self.con.execute("LOAD httpfs;")
                self.con.execute("INSTALL aws;")
                self.con.execute("LOAD aws;")
                self.con.execute(f"SET s3_region = {self.s3_region};")
                self.con.execute(f"SET s3_access_key_id='{self.credentials.access_key}';")
                self.con.execute(f"SET s3_secret_access_key='{self.credentials.secret_key}';")
                self.con.execute(f"SET s3_session_token='{self.credentials.token}';")
                self.con.execute("SET memory_limit = '7GB';")
                self.con.execute("SET temp_directory = '/tmp';")

            except duckdb.Error as e:
                logger.warning("Could not load DuckDB extensions: %s", e)

            # Create views
            self._create_views()

    def _create_views(self):
        """Create views for non-partitioned tables."""
        base_path = self.partitioned_base_path
        if not base_path.endswith("/"):
            base_path += "/"

        views_sql = f"""
        CREATE OR REPLACE VIEW catchments_partitioned AS
        SELECT * FROM read_parquet('{base_path}catchments/*/*.parquet', hive_partitioning = 1);
        
        CREATE OR REPLACE VIEW hydrotables AS
        SELECT * FROM read_parquet('{base_path}hydrotables.parquet');
        
        CREATE OR REPLACE VIEW hand_rem_rasters AS
        SELECT * FROM read_parquet('{base_path}hand_rem_rasters.parquet');
        
        CREATE OR REPLACE VIEW hand_catchment_rasters AS
        SELECT * FROM read_parquet('{base_path}hand_catchment_rasters.parquet');
        """

        for stmt in views_sql.strip().split(";"):
            if stmt.strip():
                self.con.execute(stmt.strip() + ";")

    def _partitioned_query_cte(self, wkt4326: str) -> str:
        """Returns the CTE for querying partitioned tables using direct spatial intersection."""
        return f"""
        WITH input_query AS (
          SELECT '{wkt4326}' AS wkt_string
        ),
        transformed_query AS (
          SELECT
            ST_Transform(
              ST_GeomFromText(wkt_string),
              'EPSG:4326', 'EPSG:5070', TRUE
            ) AS query_geom
          FROM input_query
        ),
        filtered_catchments AS (
          SELECT
            c.catchment_id,
            c.geometry,
            c.h3_index
          FROM catchments_partitioned c
          JOIN transformed_query tq ON ST_Intersects(ST_GeomFromWKB(c.geometry), tq.query_geom)
        )
        """

    def _get_catchment_data_for_polygon(
        self, polygon_gdf: gpd.GeoDataFrame
    ) -> Tuple[gpd.GeoDataFrame, pd.DataFrame, Optional[Polygon]]:
        """
        Query catchment data for a polygon geometry.

        Args:
            polygon_gdf: GeoDataFrame containing the query polygon

        Returns:
            Tuple of (geometries_gdf, attributes_df, query_polygon_5070)
        """
        self._ensure_connection()

        if polygon_gdf.empty:
            logger.error("Polygon GeoDataFrame is empty")
            return gpd.GeoDataFrame(), pd.DataFrame(), None

        # Ensure CRS is EPSG:4326 for injection into SQL
        if polygon_gdf.crs is None:
            logger.info("Assuming input polygon CRS = EPSG:4326")
            polygon_gdf.set_crs(epsg=4326, inplace=True)
        elif polygon_gdf.crs.to_epsg() != 4326:
            logger.info("Reprojecting query polygon to EPSG:4326")
            polygon_gdf = polygon_gdf.to_crs(epsg=4326)

        src_poly_4326 = polygon_gdf.geometry.iloc[0]
        wkt4326 = dumps(src_poly_4326)

        # Also get the same polygon in EPSG:5070 for Python overlap checks
        query_poly_5070 = polygon_gdf.to_crs(epsg=5070).geometry.iloc[0]

        # Build and run the geometry-only query using direct spatial intersection
        cte = self._partitioned_query_cte(wkt4326)
        sql_geom = (
            cte
            + """
        SELECT
          fc.catchment_id,
          fc.geometry AS geom_wkb
        FROM filtered_catchments AS fc;
        """
        )
        geom_df = self.con.execute(sql_geom).fetch_df()
        if geom_df.empty:
            logger.info("No catchments intersect the query polygon.")
            empty_gdf = gpd.GeoDataFrame(columns=["catchment_id", "geometry"], geometry="geometry", crs="EPSG:5070")
            return empty_gdf, pd.DataFrame(), query_poly_5070

        # Convert WKB data to Shapely geometry objects. Wrapping wkb in bytes because duckdb exports a bytearray but shapely wants bytes.
        geom_df["geometry"] = geom_df["geom_wkb"].apply(lambda wkb: loads(bytes(wkb)) if wkb is not None else None)
        geometries_gdf = gpd.GeoDataFrame(
            geom_df[["catchment_id", "geometry"]],
            geometry="geometry",
            crs="EPSG:5070",
        )

        # Build and run the attribute query using non-partitioned tables
        sql_attr = (
            cte
            + """
        SELECT
          fc.catchment_id,
          h.csv_path,
          hrr.raster_path AS rem_raster_path,
          hcr.raster_path AS catchment_raster_path
        FROM filtered_catchments AS fc
        LEFT JOIN hydrotables AS h ON fc.catchment_id = h.catchment_id
        LEFT JOIN hand_rem_rasters AS hrr ON fc.catchment_id = hrr.catchment_id
        LEFT JOIN hand_catchment_rasters AS hcr ON fc.catchment_id = hcr.catchment_id;
        """
        )
        attributes_df = self.con.execute(sql_attr).fetch_df()

        return geometries_gdf, attributes_df, query_poly_5070

    def _filter_dataframes_by_overlap(
        self,
        geometries_gdf: gpd.GeoDataFrame,
        attributes_df: pd.DataFrame,
        query_polygon_5070: Polygon,
    ) -> Tuple[gpd.GeoDataFrame, pd.DataFrame, Dict[str, int]]:
        """
        Filter geometries and attributes using contains/within/overlap logic.

        Selection criteria:
        - Catchments that completely contain the query polygon, OR
        - Catchments that are completely within the query polygon, OR
        - Catchments that overlap more than the threshold percentage of their own area

        Returns (filtered_geoms, filtered_attrs, summary_stats).
        """
        stats = {
            "initial_geoms": len(geometries_gdf),
            "initial_attrs": len(attributes_df),
        }
        if geometries_gdf.empty or query_polygon_5070 is None:
            stats.update(final_geoms=0, final_attrs=0, removed_geoms=0, removed_attrs=0)
            return geometries_gdf.copy(), attributes_df.copy(), stats

        geoms = geometries_gdf.copy()

        # Compute geometric relationships
        geoms["area"] = geoms.geometry.area
        geoms["inter"] = geoms.geometry.apply(
            lambda g: g.intersection(query_polygon_5070).area if not g.is_empty else 0.0
        )
        geoms["overlap_pct"] = (geoms["inter"] / geoms["area"].replace({0: pd.NA})) * 100
        geoms["overlap_pct"] = geoms["overlap_pct"].fillna(0.0)

        # Check contains/within relationships
        geoms["contains_query"] = geoms.geometry.apply(lambda g: g.contains(query_polygon_5070))
        geoms["within_query"] = geoms.geometry.apply(lambda g: g.within(query_polygon_5070))

        # Debug: log the geometric relationships
        contains_count = geoms["contains_query"].sum()
        within_count = geoms["within_query"].sum()
        logger.info(f"Geometric relationships: {contains_count} contain query, {within_count} within query")

        # Apply selection criteria: contains OR within OR overlap > threshold
        selection_mask = (
            geoms["contains_query"] | geoms["within_query"] | (geoms["overlap_pct"] >= self.overlap_threshold_percent)
        )

        keep_ids = set(geoms.loc[selection_mask, "catchment_id"])
        filtered_geoms = geoms[geoms.catchment_id.isin(keep_ids)].drop(
            columns=["area", "inter", "overlap_pct", "contains_query", "within_query"]
        )
        filtered_attrs = attributes_df[attributes_df.catchment_id.isin(keep_ids)].copy()

        # Enhanced statistics
        stats["final_geoms"] = len(filtered_geoms)
        stats["final_attrs"] = len(filtered_attrs)
        stats["removed_geoms"] = stats["initial_geoms"] - stats["final_geoms"]
        stats["removed_attrs"] = stats["initial_attrs"] - stats["final_attrs"]
        stats["contains_count"] = geoms["contains_query"].sum()
        stats["within_count"] = geoms["within_query"].sum()
        stats["overlap_only_count"] = (
            (geoms["overlap_pct"] >= self.overlap_threshold_percent) & ~geoms["contains_query"] & ~geoms["within_query"]
        ).sum()

        return filtered_geoms, filtered_attrs, stats

    def query_catchments_for_polygon(
        self, polygon_gdf: gpd.GeoDataFrame, output_dir: Optional[str] = None
    ) -> Dict[str, Dict[str, str]]:
        """
        Query catchments for a polygon and optionally write parquet files.

        Args:
            polygon_gdf: GeoDataFrame containing the query polygon
            output_dir: Optional directory to write per-catchment parquet files

        Returns:
            Dictionary mapping catchment_id to metadata including parquet_path
        """
        # Get catchment data
        geoms, attrs, query_poly_5070 = self._get_catchment_data_for_polygon(polygon_gdf)
        if geoms.empty:
            logger.info("No geometries found.")
            return {}

        # Filter by overlap
        filtered_geoms, filtered_attrs, stats = self._filter_dataframes_by_overlap(geoms, attrs, query_poly_5070)
        logger.info("Overlap filter summary: %s", stats)

        # Prepare results
        catchments = {}

        if output_dir:
            # Write parquet files
            outdir = Path(output_dir)
            outdir.mkdir(parents=True, exist_ok=True)

            for catch_id, group in filtered_attrs.groupby("catchment_id"):
                df = group.drop(columns=["catchment_id"]).copy()

                out_path = outdir / f"{catch_id}.parquet"
                df.to_parquet(str(out_path), index=False)
                logger.info("Wrote %d rows for catchment '%s' → %s", len(df), catch_id, out_path)

                catchments[catch_id] = {
                    "parquet_path": str(out_path),
                    "row_count": len(df),
                }
        else:
            # Just return metadata without writing files
            for catch_id in filtered_attrs["catchment_id"].unique():
                group = filtered_attrs[filtered_attrs["catchment_id"] == catch_id]
                catchments[catch_id] = {
                    "row_count": len(group),
                    "data": group.drop(columns=["catchment_id"]).to_dict("records"),
                }

        return catchments

    def close(self):
        """Close the DuckDB connection."""
        if self.con:
            self.con.close()
            self.con = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
