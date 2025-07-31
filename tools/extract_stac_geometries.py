#!/usr/bin/env python3
import argparse
import logging
import os
from pathlib import Path
from typing import Optional

import geopandas as gpd
import pystac_client
from shapely.geometry import shape
from shapely.ops import unary_union


def extract_geometry_by_stac_id(
    item_id: str,
    stac_api_url: str = "http://benchmark-stac.test.nextgenwaterprediction.com:8000",
    collection: Optional[str] = None,
    use_convex_hull: bool = False,
) -> gpd.GeoDataFrame:
    """
    Fetch geometry for a STAC item by its ID.

    Args:
        item_id: The STAC item ID to query for
        stac_api_url: URL of the STAC API endpoint
        collection: Optional collection to search within
        use_convex_hull: If True, compute convex hull of the geometry

    Returns:
        GeoDataFrame with the requested geometry in EPSG:4326.

    Raises:
        ValueError: if no item is found with that ID
    """
    # Connect to STAC API
    catalog = pystac_client.Client.open(stac_api_url)
    
    # Build search parameters
    search_params = {"ids": [item_id]}
    if collection:
        search_params["collections"] = [collection]
    
    # Search for the item
    search = catalog.search(**search_params)
    items = list(search.items())
    
    if not items:
        raise ValueError(f"No STAC item found with ID: {item_id}")
    
    # Get the first (should be only) item
    item = items[0]
    
    # Extract geometry
    if not item.geometry:
        raise ValueError(f"STAC item {item_id} has no geometry")
    
    # Convert to shapely geometry
    geom = shape(item.geometry)
    
    # Apply convex hull if requested
    if use_convex_hull:
        geom = geom.convex_hull
    
    # Create GeoDataFrame
    gdf = gpd.GeoDataFrame(
        [{"item_id": item_id, "collection": item.collection_id}],
        geometry=[geom],
        crs="EPSG:4326"
    )
    
    return gdf


def should_use_convex_hull(collection_id: Optional[str]) -> bool:
    """
    Determine if convex hull should be used based on collection ID.
    
    Args:
        collection_id: The collection ID from the STAC item
        
    Returns:
        True if convex hull should be applied
    """
    fim_collections = {"nws-fim-collection", "usgs-fim-collection"}
    return collection_id in fim_collections if collection_id else False


def main():
    parser = argparse.ArgumentParser(description="Extract geometries for STAC items")
    parser.add_argument("item_list", help="Text file: one STAC item ID per line")
    parser.add_argument("output_dir", help="Directory to save individual .gpkg files")
    parser.add_argument(
        "--stac-api-url",
        default="http://benchmark-stac.test.nextgenwaterprediction.com:8000",
        help="STAC API URL"
    )
    parser.add_argument(
        "--collection",
        help="Optional: specific collection to search within"
    )
    args = parser.parse_args()

    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")

    outdir = Path(args.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)

    with open(args.item_list) as fh:
        item_ids = [line.strip() for line in fh if line.strip()]

    logging.info(f"Fetching {len(item_ids)} STAC item geometries from {args.stac_api_url}")
    success = 0
    fail = 0

    for idx, item_id in enumerate(item_ids):
        try:
            logging.info(f"[{idx}] Fetching STAC item {item_id}")
            
            # First, fetch without convex hull to get collection info
            gdf = extract_geometry_by_stac_id(
                item_id,
                stac_api_url=args.stac_api_url,
                collection=args.collection,
                use_convex_hull=False
            )
            
            # Check if we should use convex hull based on collection
            collection_id = gdf.iloc[0]["collection"]
            if should_use_convex_hull(collection_id):
                logging.info(f"[{idx}] Applying convex hull for collection {collection_id}")
                gdf = extract_geometry_by_stac_id(
                    item_id,
                    stac_api_url=args.stac_api_url,
                    collection=args.collection,
                    use_convex_hull=True
                )
            
            out_fp = outdir / f"stac_{item_id}.gpkg"
            gdf.to_file(out_fp, driver="GPKG")
            logging.info(f"[{idx}] Saved {item_id} → {out_fp.name}")
            success += 1
        except Exception as e:
            logging.error(f"[{idx}] Failed {item_id}: {e}")
            fail += 1

    logging.info(f"Done: {success} succeeded, {fail} failed ({len(item_ids)} total).")


if __name__ == "__main__":
    main()