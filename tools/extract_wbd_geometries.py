#!/usr/bin/env python3
import argparse
import logging
import os
from pathlib import Path

import geopandas as gpd


def extract_geometry_by_huc(gpkg_path: str, huc_code: str) -> gpd.GeoDataFrame:
    """
    Extract geometry from WBD National gpkg for a specific HUC code.

    Args:
        gpkg_path: Path to WBD_National.gpkg
        huc_code: HUC code to extract

    Returns:
        GeoDataFrame with single geometry in EPSG:4326

    Raises:
        ValueError: If HUC code format is invalid or geometry not found
    """
    huc_length = len(huc_code)
    if huc_length == 2:
        layer_name = "WBDHU2"
        huc_field = "HUC2"
    elif huc_length == 4:
        layer_name = "WBDHU4"
        huc_field = "HUC4"
    elif huc_length == 6:
        layer_name = "WBDHU6"
        huc_field = "HUC6"
    elif huc_length == 8:
        layer_name = "WBDHU8"
        huc_field = "HUC8"
    else:
        raise ValueError(f"Invalid HUC code length {huc_length}. Expected 2, 4, 6, or 8 digits.")

    # Load geodataframe from gpkg with SQL filter
    sql_filter = f"SELECT * FROM {layer_name} WHERE {huc_field} = '{huc_code}'"
    filtered_gdf = gpd.read_file(gpkg_path, sql=sql_filter)

    if len(filtered_gdf) == 0:
        raise ValueError(f"No polygon found for HUC code {huc_code} in layer {layer_name}")

    if filtered_gdf.crs and filtered_gdf.crs.to_epsg() != 4326:
        filtered_gdf = filtered_gdf.to_crs("EPSG:4326")

    return filtered_gdf


def main():
    parser = argparse.ArgumentParser(description="Extract individual HUC geometries from WBD National dataset")
    parser.add_argument("wbd_gpkg", help="Path to WBD_National.gpkg")
    parser.add_argument("huc_list", help="Path to text file containing HUC codes (one per line)")
    parser.add_argument("output_dir", help="Directory to save individual polygon gpkg files")

    args = parser.parse_args()

    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")

    output_path = Path(args.output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    with open(args.huc_list, "r") as f:
        huc_codes = [line.strip() for line in f if line.strip()]

    logging.info(f"Processing {len(huc_codes)} HUC codes")

    successful = 0
    failed = 0

    for i, huc_code in enumerate(huc_codes):
        try:
            logging.info(f"[{i}] Extracting geometry for HUC {huc_code}")

            gdf = extract_geometry_by_huc(args.wbd_gpkg, huc_code)

            output_file = output_path / f"huc_{huc_code}.gpkg"
            gdf.to_file(output_file, driver="GPKG")

            logging.info(f"[{i}] Saved {huc_code} to {output_file}")
            successful += 1

        except Exception as e:
            logging.error(f"[{i}] Failed to process HUC {huc_code}: {e}")
            failed += 1

    logging.info(f"\nProcessing complete:")
    logging.info(f"  Successful: {successful}")
    logging.info(f"  Failed: {failed}")
    logging.info(f"  Total: {successful + failed}")


if __name__ == "__main__":
    main()
