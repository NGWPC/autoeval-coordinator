import duckdb
import geopandas as gpd
import pandas as pd
from shapely.geometry import shape, Polygon
from shapely.wkt import dumps
from typing import Tuple, Dict


def _common_cte(wkt4326: str) -> str:
    return f"""
    WITH input_query AS (
      SELECT '{wkt4326}' AS wkt_string
    ), transformed_query AS (
      SELECT
        ST_Transform(
          ST_GeomFromText(wkt_string),
          'EPSG:4326', 'EPSG:5070', TRUE
        ) AS query_geom
      FROM input_query
    ), filtered_catchments AS (
      SELECT DISTINCT
        c.catchment_id,
        c.geometry
      FROM catchments AS c
      JOIN transformed_query AS tq
        ON ST_Intersects(c.geometry, tq.query_geom)
    )
    """


def get_catchment_data_for_polygon(
    polygon_geojson: Dict, con: duckdb.DuckDBPyConnection
) -> Tuple[gpd.GeoDataFrame, pd.DataFrame, Polygon]:
    """
    Run two DuckDB spatial queries against `catchments`:

      - one for the geometries (to decode WKB → GeoDataFrame)
      - one for the attribute table
    """
    # Convert geojson dict → shapely geometry (assume Feature or pure GeoJSON geometry)
    if polygon_geojson.get("type") == "Feature":
        geom = shape(polygon_geojson["geometry"])
    else:
        geom = shape(polygon_geojson)

    # Prepare WKT in 4326
    wkt4326 = dumps(geom)

    # Also get the same polygon in EPSG:5070 for any python‐side checks
    query_poly_5070 = gpd.GeoSeries([geom], crs="EPSG:4326").to_crs(epsg=5070).iloc[0]

    cte = _common_cte(wkt4326)

    # 1) geometry‐only query
    sql_geom = (
        cte
        + """
    SELECT
      fc.catchment_id,
      ST_AsWKB(fc.geometry) AS geom_wkb
    FROM filtered_catchments AS fc;
    """
    )
    geom_df = con.execute(sql_geom).fetch_df()

    if geom_df.empty:
        empty_gdf = gpd.GeoDataFrame(
            columns=["catchment_id", "geometry"], geometry="geometry", crs="EPSG:5070"
        )
        return empty_gdf, pd.DataFrame(), query_poly_5070

    # decode
    wkb_series = geom_df["geom_wkb"].apply(
        lambda x: bytes(x) if isinstance(x, bytearray) else x
    )
    geometries_gdf = gpd.GeoDataFrame(
        geom_df[["catchment_id"]],
        geometry=gpd.GeoSeries.from_wkb(wkb_series, crs="EPSG:5070"),
        crs="EPSG:5070",
    )

    # 2) attribute query
    sql_attr = (
        cte
        + """
    SELECT
      f.catchment_id,
      ht.* EXCLUDE (catchment_id),
      hrr.raster_path   AS rem_raster_path,
      hcr.raster_path   AS catchment_raster_path
    FROM filtered_catchments AS f
    LEFT JOIN hydrotables             AS ht   ON f.catchment_id = ht.catchment_id
    LEFT JOIN hand_rem_rasters       AS hrr  ON f.catchment_id = hrr.catchment_id
    LEFT JOIN hand_catchment_rasters AS hcr  ON hrr.rem_raster_id = hcr.rem_raster_id;
    """
    )
    attributes_df = con.execute(sql_attr).fetch_df()

    return geometries_gdf, attributes_df, query_poly_5070
