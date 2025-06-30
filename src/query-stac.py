#!/usr/bin/env python3
import argparse
import json
import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Pattern, Tuple

import geopandas as gpd
import requests
from pystac_client import Client
from shapely.geometry import Polygon, shape

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_iso(ts: str) -> datetime:
    """
    Strict ISO‐8601 → datetime with tzinfo.
    Replace trailing 'Z' so fromisoformat handles UTC.
    """
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts)


def dictify(o: Any) -> Any:
    """
    Recursively convert defaultdict → dict for JSON serialization.
    (For nested dataclass support you could use dataclasses-json [github.com].)
    """
    if isinstance(o, defaultdict):
        o = dict(o)
    if isinstance(o, dict):
        return {k: dictify(v) for k, v in o.items()}
    if isinstance(o, list):
        return [dictify(v) for v in o]
    return o


def compare_versions(v1: str, v2: str) -> int:
    """Compare two flows2fim_version strings (e.g., '0_3_0' vs '0_10_3')
    Returns 1 if v1 > v2, -1 if v1 < v2, 0 if equal."""
    try:
        v1_parts = [int(p) for p in v1.split("_")]
        v2_parts = [int(p) for p in v2.split("_")]

        for i in range(min(len(v1_parts), len(v2_parts))):
            if v1_parts[i] > v2_parts[i]:
                return 1
            elif v1_parts[i] < v2_parts[i]:
                return -1

        # If we've compared all parts and they're equal, the longer one is newer
        if len(v1_parts) > len(v2_parts):
            return 1
        elif len(v1_parts) < len(v2_parts):
            return -1
        return 0
    except (ValueError, AttributeError):
        # If versions cannot be compared, treat them as equal
        return 0


def filter_items_by_geometry(
    items: List[Any], query_polygon: Polygon, overlap_threshold_percent: float = 40.0
) -> List[Any]:
    """
    Filter STAC items using geometry relationships similar to HAND query filtering.

    Selection criteria:
    - Items that completely contain the query polygon, OR
    - Items that are completely within the query polygon, OR
    - Items that overlap more than the threshold percentage of their own area

    Args:
        items: List of STAC items with geometry property
        query_polygon: Shapely polygon for filtering (assumed to be in same CRS as items)
        overlap_threshold_percent: Minimum overlap percentage to keep an item

    Returns:
        Filtered list of STAC items
    """
    if not items or not query_polygon:
        return items

    filtered_items = []
    stats = {
        "initial_count": len(items),
        "contains_count": 0,
        "within_count": 0,
        "overlap_only_count": 0,
        "removed_count": 0,
    }

    for item in items:
        try:
            # Convert STAC item geometry to shapely
            if not hasattr(item, "geometry") or not item.geometry:
                # Skip items without geometry
                filtered_items.append(item)
                continue

            item_geom = shape(item.geometry)
            if item_geom.is_empty:
                continue

            # Compute geometric relationships
            contains_query = item_geom.contains(query_polygon)
            within_query = item_geom.within(query_polygon)

            # Compute overlap percentage relative to item's area
            if not contains_query and not within_query:
                intersection_area = item_geom.intersection(query_polygon).area
                item_area = item_geom.area
                if item_area > 0:
                    overlap_pct = (intersection_area / item_area) * 100
                else:
                    overlap_pct = 0.0
            else:
                overlap_pct = 100.0  # Contains or within means 100% relevant

            # Apply selection criteria
            if contains_query:
                filtered_items.append(item)
                stats["contains_count"] += 1
            elif within_query:
                filtered_items.append(item)
                stats["within_count"] += 1
            elif overlap_pct >= overlap_threshold_percent:
                filtered_items.append(item)
                stats["overlap_only_count"] += 1
            else:
                stats["removed_count"] += 1

        except Exception as e:
            logger.warning(f"Error processing item geometry for {getattr(item, 'id', 'unknown')}: {e}")
            # Include item if geometry processing fails
            filtered_items.append(item)

    logger.info(
        f"Geometry filter results: {stats['initial_count']} initial, "
        f"{stats['contains_count']} contain query, {stats['within_count']} within query, "
        f"{stats['overlap_only_count']} overlap only, {stats['removed_count']} removed, "
        f"{len(filtered_items)} final"
    )

    return filtered_items


BLE_SPEC: List[Tuple[Pattern, str, str]] = [
    (re.compile(r"^(\d+yr)_(extent_raster|flow_file)$"), r"\1", r"\2"),
]
NWS_USGS_MAGS = ["action", "minor", "moderate", "major"]
NWS_USGS_SPEC = [(re.compile(rf"^{mag}_(extent_raster|flow_file)$"), mag, r"\1") for mag in NWS_USGS_MAGS]
RIPPLE_SPEC: List[Tuple[Pattern, str, str]] = [(re.compile(r"^(\d+yr)_extent$"), r"\1", "extents")]


CollectionConfig = Dict[
    str,
    Tuple[
        Callable[[Any], str],  # grouping fn → group_id
        Dict[str, Callable[[str, Any], bool]],  # asset_type → test(key, asset)
    ],
]

COLLECTIONS: CollectionConfig = {
    "gfm-collection": (
        lambda item: str(item.properties.get("dfo_event_id", item.id)),
        {
            "extents": lambda k, a: k.endswith("_Observed_Water_Extent"),
            "flowfiles": lambda k, a: k.endswith("_flowfile"),
        },
    ),
    "gfm-expanded-collection": (
        lambda item: group_gfm_expanded_initial(item),
        {
            "extents": lambda k, a: k.endswith("_Observed_Water_Extent"),
            "flowfiles": lambda k, a: k.endswith("_flowfile") or k == "NWM_ANA_flowfile",
        },
    ),
    "hwm-collection": (
        lambda item: item.id,
        {
            "points": lambda k, a: k == "data" and a.media_type and "geopackage" in a.media_type,
            "flowfiles": lambda k, a: k.endswith("-flowfile"),
        },
    ),
}


def group_gfm_expanded_initial(item: Any) -> str:
    start = item.datetime
    if not start:
        sp = item.properties.get("gfm_data_take_start_datetime")
        if sp:
            start = parse_iso(sp)
        else:
            logger.warning(f"No start for {item.id}; using item.id")
            return item.id
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)

    end_val = item.properties.get("end_datetime")
    if isinstance(end_val, str):
        end = parse_iso(end_val)
    else:
        end = end_val or start

    if end < start:
        ep = item.properties.get("gfm_data_take_end_datetime")
        try:
            end = parse_iso(ep) if ep else start
        except Exception:
            end = start

    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    fmt = "%Y-%m-%dT%H:%M:%SZ"
    return f"{start.strftime(fmt)}/{end.strftime(fmt)}"


@dataclass(order=True)
class Interval:
    start: datetime
    end: datetime
    assets: Dict[str, List[str]] = field(default_factory=lambda: defaultdict(list))


def format_results(item_iter: Any) -> Dict[str, Dict[str, Dict[str, List[str]]]]:
    results = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    seen = set()
    ripple_cache: Dict[str, List[str]] = {}
    ripple_best_items = {}

    for idx, item in enumerate(item_iter, start=1):
        if item.collection_id == "ripple-fim-collection":
            try:
                source = item.properties.get("source", "")
                hucs = tuple(item.properties.get("hucs", []))
                flows2fim_version = item.properties.get("flows2fim_version", "")

                if source and hucs and flows2fim_version:
                    key = (source, hucs)

                    # If we haven't seen this source+hucs combo, or this version is better
                    if (
                        key not in ripple_best_items
                        or compare_versions(flows2fim_version, ripple_best_items[key][1]) > 0
                    ):
                        ripple_best_items[key] = (item.id, flows2fim_version)
                    else:
                        # Skip this item - we already have a better version
                        logger.info(f"Skipping {item.id} - newer version exists for {source}+{hucs[0] if hucs else ''}")
                        continue
            except Exception as e:
                logger.warning(f"Error processing Ripple item {item.id}: {e}")

        if item.id in seen:
            continue
        seen.add(item.id)
        if idx % 100 == 0:
            logger.info(f"Processed {idx} items (last: {item.id})")

        coll = item.collection_id or "<none>"
        short = coll.replace("-collection", "").replace("-fim", "")
        if short == "gfm-expanded":
            short = "gfm_expanded"

        # 1) item‐level grouping
        if coll in COLLECTIONS:
            group_fn, tests = COLLECTIONS[coll]
            gid = group_fn(item)
            bucket = results[short][gid]
            for k, a in item.assets.items():
                if not a.href:
                    continue
                for atype, test in tests.items():
                    if test(k, a) and a.href not in bucket[atype]:
                        bucket[atype].append(a.href)

        # 2) BLE/NWS/USGS/Ripple asset‐level grouping
        elif coll == "ble-collection" or coll.endswith("-fim-collection"):
            # preload ripple assets once
            if coll == "ripple-fim-collection" and not ripple_cache:
                try:
                    col = item.get_collection()
                    for ak, aa in col.assets.items():
                        m = re.search(r"flows_(\d+)_yr_", ak)
                        ri = f"{m.group(1)}yr" if m else None
                        if ri and aa.media_type == "text/csv":
                            logger.info(f"Caching Ripple flowfile for {ri}: {aa.href}")
                            ripple_cache.setdefault(ri, []).append(aa.href)
                except Exception as e:
                    logger.warning(f"Ripple cache failed: {e}")

            specs = (
                BLE_SPEC
                if coll == "ble-collection"
                else RIPPLE_SPEC
                if coll == "ripple-fim-collection"
                else NWS_USGS_SPEC
            )

            found = set()
            for k, a in item.assets.items():
                if not a.href:
                    continue
                for pat, gid_t, at_t in specs:
                    m = pat.match(k)
                    if not m:
                        continue
                    gid = m.expand(gid_t) if "\\" in gid_t else gid_t
                    at = m.expand(at_t)
                    bkt = results[short][gid]
                    if a.href not in bkt[at]:
                        bkt[at].append(a.href)
                    found.add(gid)

            # append ripple flowfiles
            if coll == "ripple-fim-collection":
                for gid in found:
                    if gid in ripple_cache:
                        logger.info(f"Adding cached flowfiles for {gid}")
                        for href in ripple_cache[gid]:
                            if href not in results[short][gid]["flowfiles"]:
                                results[short][gid]["flowfiles"].append(href)

        # 3) fallback
        else:
            logger.warning(f"Unknown coll '{coll}'; grouping by item.id")
            gid = item.id
            bkt = results[short][gid]
            for k, a in item.assets.items():
                if not a.href:
                    continue
                if "extent" in k and a.media_type and "tiff" in a.media_type:
                    bkt["extents"].append(a.href)
                elif "flow" in k and a.media_type and "csv" in a.media_type:
                    bkt["flowfiles"].append(a.href)

    logger.info(f"Finished formatting {len(seen)} items.")
    return results


# Set tolerancce days for observation merging to 3 days because that is the outer limit for how long it should take the two sentinal 1 satellite's swaths to cover the continental US when operating under normal conditions and creating flood maps from both the ascending and descending orbits (which is what GFM does). Even if an event continues past that 3 days would still probably want to subset as another FIM grouping since the flow conditions will likely have changed dramatically by then.
def merge_gfm_expanded(
    groups: Dict[str, Dict[str, List[str]]], tolerance_days: int = 3
) -> Dict[str, Dict[str, List[str]]]:
    if not groups:
        return {}

    tol = timedelta(days=tolerance_days)
    ivs: List[Interval] = []

    for key, data in groups.items():
        try:
            s, e = key.split("/")
            st = parse_iso(s)
            en = parse_iso(e)
            iv = Interval(start=st, end=en)
            for at, hs in data.items():
                for h in hs:
                    if h not in iv.assets[at]:
                        iv.assets[at].append(h)
            ivs.append(iv)
        except Exception as ex:
            logger.warning(f"Bad GFM‐Expanded key '{key}': {ex}")

    ivs.sort()
    merged: List[Interval] = []
    cur = ivs[0]
    for nxt in ivs[1:]:
        if nxt.start <= cur.end + tol:
            cur.end = max(cur.end, nxt.end)
            for at, hs in nxt.assets.items():
                for h in hs:
                    if h not in cur.assets[at]:
                        cur.assets[at].append(h)
        else:
            merged.append(cur)
            cur = nxt
    merged.append(cur)

    out: Dict[str, Dict[str, List[str]]] = {}
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    for iv in merged:
        key = f"{iv.start.strftime(fmt)}/{iv.end.strftime(fmt)}"
        out[key] = iv.assets
    return out


def main() -> None:
    p = argparse.ArgumentParser("Group STAC flood items into extents/flowfiles")
    p.add_argument("-u", "--api-url", required=True, help="STAC API root URL")
    p.add_argument(
        "-c",
        "--collections",
        nargs="+",
        required=True,
        help="One or more STAC collection IDs",
    )
    p.add_argument(
        "-r",
        "--roi",
        type=Path,
        help="GeoJSON file with exactly one Polygon/MultiPolygon",
    )
    p.add_argument("-d", "--datetime", help="STAC datetime or interval")
    p.add_argument("-o", "--output-file", help="Write JSON output to this path")
    p.add_argument(
        "-t",
        "--overlap-threshold",
        type=float,
        default=40.0,
        help="Minimum overlap percentage to keep a STAC item (default: 40.0)",
    )
    args = p.parse_args()

    # load & validate single‐feature ROI
    intersects = None
    query_polygon = None
    if args.roi:
        gj = json.loads(args.roi.read_text(encoding="utf-8"))
        t = gj.get("type")
        if t == "FeatureCollection":
            feats = gj.get("features", [])
            if len(feats) != 1:
                logger.error("ROI FC must contain exactly one feature.")
                return
            intersects = feats[0]["geometry"]
        elif t == "Feature":
            intersects = gj["geometry"]
        elif t in ("Polygon", "MultiPolygon"):
            intersects = gj
        else:
            logger.error("ROI must be FC(1 feature), Feature, or Polygon/MultiPolygon.")
            return

        # Convert to shapely polygon for geometry filtering
        try:
            query_polygon = shape(intersects)
            logger.info("Loaded single‐feature ROI for intersects filter and geometry filtering")
        except Exception as e:
            logger.error(f"Could not convert ROI to shapely geometry: {e}")
            return

    # open STAC client
    try:
        client = Client.open(args.api_url)
        logger.info(f"Connected to {args.api_url}")
    except Exception as ex:
        logger.error(f"Could not open STAC API: {ex}")
        return

    # build search kwargs (no max_items → all items)
    search_kw = {
        "collections": args.collections,
        "datetime": args.datetime,
        **({"intersects": intersects} if intersects else {}),
    }
    search_kw = {k: v for k, v in search_kw.items() if v is not None}

    try:
        logger.info(f"Searching collections {search_kw['collections']}")
        search = client.search(**search_kw)
        items = list(search.items())  # Convert to list for geometry filtering

        # Apply geometry filtering if ROI is provided
        if query_polygon and args.overlap_threshold:
            logger.info(f"Applying geometry filtering with {args.overlap_threshold}% overlap threshold")
            items = filter_items_by_geometry(items, query_polygon, args.overlap_threshold)

        grouped = format_results(items)
        if "gfm_expanded" in grouped:
            grouped["gfm_expanded"] = merge_gfm_expanded(grouped["gfm_expanded"])

        out = dictify(grouped)
        text = json.dumps(out, indent=2)

        if args.output_file:
            Path(args.output_file).write_text(text, encoding="utf-8")
            logger.info(f"Wrote to {args.output_file}")
        else:
            print(text)

    except requests.RequestException as rex:
        logger.error(f"STAC request failed: {rex}")
    except Exception as ex:
        logger.exception(f"Unexpected error: {ex}")


if __name__ == "__main__":
    """
    Group STAC flood‐model items into extents/flowfiles by:
      • gfm          → group by dfo_event_id
      • gfm‐expanded → initial time‐range, then merge close intervals
      • hwm          → each item is its own survey
      • ble/nws/ripple/usgs → asset‐level grouping by regex

    Spatial filter via a single‐feature GeoJSON ROI (--roi).
    """

    main()
