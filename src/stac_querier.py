import json
import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Pattern, Tuple

import geopandas as gpd
import requests
from pystac_client import Client
from shapely.geometry import Polygon, shape

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


@dataclass(order=True)
class Interval:
    start: datetime
    end: datetime
    assets: Dict[str, List[str]] = field(default_factory=lambda: defaultdict(list))


class StacQuerier:
    """
    A class for querying STAC APIs and processing flood model items.
    Provides spatial filtering and grouping capabilities.
    """

    def __init__(
        self,
        api_url: str,
        collections: Optional[List[str]] = None,
        overlap_threshold_percent: float = 40.0,
        datetime_filter: Optional[str] = None,
    ):
        """
        Initialize the StacQuerier.

        Args:
            api_url: STAC API root URL
            collections: List of STAC collection IDs to query (None means query all available)
            overlap_threshold_percent: Minimum overlap percentage to keep a STAC item
            datetime_filter: STAC datetime or interval filter
        """
        self.api_url = api_url
        self.collections = collections
        self.overlap_threshold_percent = overlap_threshold_percent
        self.datetime_filter = datetime_filter
        self.client = None

        # Collection specifications
        self.BLE_SPEC: List[Tuple[Pattern, str, str]] = [
            (re.compile(r"^(\d+yr)_(extent_raster|flow_file)$"), r"\1", r"\2"),
        ]
        self.NWS_USGS_MAGS = ["action", "minor", "moderate", "major"]
        self.NWS_USGS_SPEC = [
            (re.compile(rf"^{mag}_(extent_raster|flow_file)$"), mag, r"\1") for mag in self.NWS_USGS_MAGS
        ]
        self.RIPPLE_SPEC: List[Tuple[Pattern, str, str]] = [(re.compile(r"^(\d+yr)_extent$"), r"\1", "extents")]

        CollectionConfig = Dict[
            str,
            Tuple[
                Callable[[Any], str],  # grouping fn → group_id
                Dict[str, Callable[[str, Any], bool]],  # asset_type → test(key, asset)
            ],
        ]

        self.COLLECTIONS: CollectionConfig = {
            "gfm-collection": (
                lambda item: str(item.properties.get("dfo_event_id", item.id)),
                {
                    "extents": lambda k, a: k.endswith("_Observed_Water_Extent"),
                    "flowfiles": lambda k, a: k.endswith("_flowfile"),
                },
            ),
            "gfm-expanded-collection": (
                self._group_gfm_expanded_initial,
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

    def _ensure_client(self):
        """Ensure STAC client is connected."""
        if self.client is None:
            try:
                self.client = Client.open(self.api_url)
                logger.info(f"Connected to STAC API: {self.api_url}")
            except Exception as e:
                logger.error(f"Could not open STAC API: {e}")
                raise

    def _filter_items_by_geometry(self, items: List[Any], query_polygon: Polygon) -> List[Any]:
        """
        Filter STAC items using geometry relationships similar to HAND query filtering.

        Selection criteria:
        - Items that completely contain the query polygon, OR
        - Items that are completely within the query polygon, OR
        - Items that overlap more than the threshold percentage of their own area

        Args:
            items: List of STAC items with geometry property
            query_polygon: Shapely polygon for filtering (assumed to be in same CRS as items)

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
                elif overlap_pct >= self.overlap_threshold_percent:
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

    def _group_gfm_expanded_initial(self, item: Any) -> str:
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

    def _format_results(self, item_iter: Any) -> Dict[str, Dict[str, Dict[str, List[str]]]]:
        results = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        seen = set()
        ripple_cache: Dict[str, List[str]] = {}
        ripple_best_items = {}
        # Track STAC item IDs for each scenario
        item_ids = defaultdict(lambda: defaultdict(set))
        gauge_info = defaultdict(lambda: defaultdict(lambda: None))

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
                            logger.info(
                                f"Skipping {item.id} - newer version exists for {source}+{hucs[0] if hucs else ''}"
                            )
                            continue
                except Exception as e:
                    logger.warning(f"Error processing Ripple item {item.id}: {e}")

            if item.id in seen:
                continue
            seen.add(item.id)
            if idx % 100 == 0:
                logger.info(f"Processed {idx} items (last: {item.id})")

            coll = item.collection_id or "<none>"
            collection_key = coll
            # Handle special case for gfm-expanded-collection to maintain compatibility
            if coll == "gfm-expanded-collection":
                collection_key = "gfm_expanded"

            # 1) item‐level grouping
            if coll in self.COLLECTIONS:
                group_fn, tests = self.COLLECTIONS[coll]
                gid = group_fn(item)
                bucket = results[collection_key][gid]
                item_ids[collection_key][gid].add(item.id)
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
                    self.BLE_SPEC
                    if coll == "ble-collection"
                    else self.RIPPLE_SPEC
                    if coll == "ripple-fim-collection"
                    else self.NWS_USGS_SPEC
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
                        bkt = results[collection_key][gid]
                        item_ids[collection_key][gid].add(item.id)
                        if a.href not in bkt[at]:
                            bkt[at].append(a.href)
                        found.add(gid)

                        if coll in ["nws-fim-collection", "usgs-fim-collection"]:
                            gauge = item.properties.get("gauge")
                            if gauge and gauge_info[collection_key][gid] is None:
                                gauge_info[collection_key][gid] = gauge

                # append ripple flowfiles
                if coll == "ripple-fim-collection":
                    for gid in found:
                        if gid in ripple_cache:
                            logger.info(f"Adding cached flowfiles for {gid}")
                            for href in ripple_cache[gid]:
                                if href not in results[collection_key][gid]["flowfiles"]:
                                    results[collection_key][gid]["flowfiles"].append(href)

            # 3) fallback
            else:
                logger.warning(f"Unknown coll '{coll}'; grouping by item.id")
                gid = item.id
                bkt = results[collection_key][gid]
                item_ids[collection_key][gid].add(item.id)

                if "nws" in coll.lower() or "usgs" in coll.lower():
                    gauge = item.properties.get("gauge")
                    if gauge and gauge_info[collection_key][gid] is None:
                        gauge_info[collection_key][gid] = gauge

                for k, a in item.assets.items():
                    if not a.href:
                        continue
                    if "extent" in k and a.media_type and "tiff" in a.media_type:
                        bkt["extents"].append(a.href)
                    elif "flow" in k and a.media_type and "csv" in a.media_type:
                        bkt["flowfiles"].append(a.href)

        # Add item IDs and gauge information to results
        for collection_key in results:
            for scenario_id in results[collection_key]:
                results[collection_key][scenario_id]["stac_items"] = list(item_ids[collection_key][scenario_id])
                results[collection_key][scenario_id]["gauge"] = gauge_info[collection_key][scenario_id]

        logger.info(f"Finished formatting {len(seen)} items.")
        return results

    def _merge_gfm_expanded(
        self, groups: Dict[str, Dict[str, List[str]]], tolerance_days: int = 3
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
                    if at == "stac_items":
                        # Handle stac_items specially - they're a list, not nested lists
                        for h in hs:
                            if h not in iv.assets[at]:
                                iv.assets[at].append(h)
                    elif at == "gauge":
                        # Handle gauge specially - it's a single value, not a list
                        if hs is not None:
                            iv.assets[at] = hs
                    else:
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
                    if at == "stac_items":
                        # Handle stac_items specially - they're a list, not nested lists
                        for h in hs:
                            if h not in cur.assets[at]:
                                cur.assets[at].append(h)
                    elif at == "gauge":
                        # Handle gauge specially - use the first non-null value
                        if hs is not None and at not in cur.assets:
                            cur.assets[at] = hs
                    else:
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

    def query_stac_for_polygon(
        self, polygon_gdf: Optional[gpd.GeoDataFrame] = None, roi_geojson: Optional[Dict] = None
    ) -> Dict[str, Dict[str, Dict[str, List[str]]]]:
        """
        Query STAC API for items and group them by collection/scenario.

        Args:
            polygon_gdf: GeoDataFrame containing the query polygon (optional)
            roi_geojson: GeoJSON dictionary for spatial filtering (optional)

        Returns:
            Dictionary mapping collection -> scenario -> asset_type -> [paths]
            Each scenario also includes 'stac_items' key with list of STAC item IDs
        """
        self._ensure_client()

        # Prepare spatial filter
        intersects = None
        query_polygon = None

        if polygon_gdf is not None and not polygon_gdf.empty:
            # Convert GeoDataFrame to GeoJSON
            geom = polygon_gdf.geometry.iloc[0]
            # Ensure it's in WGS84 for STAC query
            if polygon_gdf.crs and polygon_gdf.crs.to_epsg() != 4326:
                polygon_gdf_4326 = polygon_gdf.to_crs(epsg=4326)
                geom = polygon_gdf_4326.geometry.iloc[0]

            intersects = geom.__geo_interface__
            query_polygon = geom

        elif roi_geojson:
            intersects = roi_geojson
            try:
                query_polygon = shape(roi_geojson)
            except Exception as e:
                logger.warning(f"Could not convert ROI to shapely geometry: {e}")

        # Build search kwargs
        search_kw = {
            "datetime": self.datetime_filter,
            **({"intersects": intersects} if intersects else {}),
        }

        # KLUDGE: Hardcode 2022 date range for GFM collections. this will be removed after trial batches with gfm data are completed.
        if self.collections is not None and any("gfm" in coll.lower() for coll in self.collections):
            search_kw["datetime"] = "2022-01-01T00:00:00Z/2022-12-31T23:59:59Z"
            logger.info("KLUDGE: Overriding datetime filter to 2022 for GFM collections")

        # Only include collections if specified, otherwise query all available
        if self.collections is not None:
            search_kw["collections"] = self.collections

        search_kw = {k: v for k, v in search_kw.items() if v is not None}

        try:
            collections_msg = search_kw.get("collections", "all available collections")
            logger.info(f"Searching collections {collections_msg}")
            search = self.client.search(**search_kw)
            items = list(search.items())  # Convert to list for geometry filtering
            
            if not items:
                logger.info("STAC query returned no items")
                return {}

            # Apply geometry filtering if query polygon is provided
            if query_polygon and self.overlap_threshold_percent:
                logger.info(f"Applying geometry filtering with {self.overlap_threshold_percent}% overlap threshold")
                items = self._filter_items_by_geometry(items, query_polygon)
                if not items:
                    logger.info("No items remained after geometry filtering")
                    return {}

            grouped = self._format_results(items)
            
            if not grouped:
                logger.info("No valid scenarios found after processing STAC items")
                return {}
            if "gfm_expanded" in grouped:
                grouped["gfm_expanded"] = self._merge_gfm_expanded(grouped["gfm_expanded"])

                # Truncate timestamp ranges to just the first timestamp to get rid of double directory writing issue
                truncated = {}
                for scenario_key, scenario_data in grouped["gfm_expanded"].items():
                    # If key contains a slash (timestamp range), take only the first part
                    if "/" in scenario_key:
                        first_timestamp = scenario_key.split("/")[0]
                        truncated[first_timestamp] = scenario_data
                    else:
                        truncated[scenario_key] = scenario_data
                grouped["gfm_expanded"] = truncated

            return dictify(grouped)

        except requests.RequestException as rex:
            logger.error(f"STAC request failed: {rex}")
            raise
        except Exception as ex:
            logger.error(f"Unexpected error: {ex}")
            raise

    def save_results(self, results: Dict, output_path: str):
        """Save query results to JSON file."""
        with open(output_path, "w") as f:
            json.dump(results, f, indent=2)
        logger.info(f"STAC query results saved to: {output_path}")
