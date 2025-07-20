#!/usr/bin/env python3
import argparse
import logging
import os
from pathlib import Path

import geopandas as gpd
from pygeohydro import WBD  # client to pull WBD geometries


def extract_geometry_by_huc(huc_code: str) -> gpd.GeoDataFrame:
    """
    Fetch a single HUC polygon (2–12 digit) from the WBD REST service.

    Args:
        huc_code: 2, 4, 6, 8, 10 or 12 digit HUC.

    Returns:
        GeoDataFrame with the requested HUC polygon in EPSG:4326.

    Raises:
        ValueError: if the service returns no geometry for that HUC.
    """
    # Determine the layer based on HUC code length
    huc_len = len(str(huc_code))
    layer_map = {
        2: "huc2",
        4: "huc4", 
        6: "huc6",
        8: "huc8",
        10: "huc10",
        12: "huc12"
    }
    
    if huc_len not in layer_map:
        raise ValueError(f"Invalid HUC code length: {huc_len}")
    
    layer = layer_map[huc_len]
    client = WBD(layer=layer)  # initialize the WBD client with appropriate layer
    
    # Query for the specific HUC using byids
    gdf = client.byids(f"huc{huc_len}", [huc_code])

    if gdf.empty:
        raise ValueError(f"No polygon found for HUC code {huc_code}")

    # ensure it’s in WGS84
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(4326)

    return gdf


def main():
    parser = argparse.ArgumentParser(description="Extract individual HUC (2–12) geometries via pygeohydro")
    parser.add_argument("huc_list", help="Text file: one HUC code (2–12 digits) per line")
    parser.add_argument("output_dir", help="Directory to save individual .gpkg files")
    args = parser.parse_args()

    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")

    outdir = Path(args.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)

    with open(args.huc_list) as fh:
        hucs = [line.strip() for line in fh if line.strip()]

    logging.info(f"Fetching {len(hucs)} HUC geometries via pygeohydro")
    success = 0
    fail = 0

    for idx, huc in enumerate(hucs):
        try:
            logging.info(f"[{idx}] Fetching HUC {huc}")
            gdf = extract_geometry_by_huc(huc)
            out_fp = outdir / f"huc_{huc}.gpkg"
            gdf.to_file(out_fp, driver="GPKG")
            logging.info(f"[{idx}] Saved {huc} → {out_fp.name}")
            success += 1
        except Exception as e:
            logging.error(f"[{idx}] Failed {huc}: {e}")
            fail += 1

    logging.info(f"Done: {success} succeeded, {fail} failed ({len(hucs)} total).")


if __name__ == "__main__":
    main()
