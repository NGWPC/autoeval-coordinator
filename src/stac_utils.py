import re
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Pattern, Tuple

logger = logging.getLogger(__name__)


def parse_iso(ts: str) -> datetime:
    """Strict ISO‐8601 → datetime with tzinfo.  Replace trailing 'Z' → '+00:00'."""
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
    """Compare two version strings like '0_3_0' vs '0_10_3'."""
    try:
        p1 = [int(x) for x in v1.split("_")]
        p2 = [int(x) for x in v2.split("_")]
        for a, b in zip(p1, p2):
            if a > b:
                return 1
            if a < b:
                return -1
        return (len(p1) > len(p2)) - (len(p1) < len(p2))
    except Exception:
        return 0


# Regex specs for BLE, NWS/USGS, Ripple asset‐level grouping
BLE_SPEC = [
    (re.compile(r"^(\d+yr)_(extent_raster|flow_file)$"), r"\1", r"\2"),
]
NWS_USGS_MAGS = ["action", "minor", "moderate", "major"]
NWS_USGS_SPEC = [
    (re.compile(rf"^{mag}_(extent_raster|flow_file)$"), mag, r"\1")
    for mag in NWS_USGS_MAGS
]
RIPPLE_SPEC = [
    (re.compile(r"^(\d+yr)_extent$"), r"\1", "extents"),
]

# Collection‐level grouping rules
CollectionConfig = Dict[
    str,
    Tuple[
        # → group_id fn
        Callable[[Any], str],
        # asset_type → test(k, asset) fn
        Dict[str, Callable[[str, Any], bool]],
    ],
]


def group_gfm_expanded_initial(item: Any) -> str:
    """
    Build initial time‐range key for gfm-expanded:
      start/end from item.datetime or properties.
    """
    start = item.datetime or parse_iso(
        item.properties.get("gfm_data_take_start_datetime", "")
    )
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)

    end_raw = item.properties.get("end_datetime") or item.properties.get(
        "gfm_data_take_end_datetime"
    )
    end = parse_iso(end_raw) if isinstance(end_raw, str) else (end_raw or start)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    fmt = "%Y-%m-%dT%H:%M:%SZ"
    return f"{start.strftime(fmt)}/{end.strftime(fmt)}"


COLLECTIONS: CollectionConfig = {
    "gfm-collection": (
        lambda i: str(i.properties.get("dfo_event_id", i.id)),
        {
            "extents": lambda k, a: k.endswith("_Observed_Water_Extent"),
            "flowfiles": lambda k, a: k.endswith("_flowfile"),
        },
    ),
    "gfm-expanded-collection": (
        group_gfm_expanded_initial,
        {
            "extents": lambda k, a: k.endswith("_Observed_Water_Extent"),
            "flowfiles": lambda k, a: k.endswith("_flowfile")
            or k == "NWM_ANA_flowfile",
        },
    ),
    "hwm-collection": (
        lambda i: i.id,
        {
            "points": lambda k, a: k == "data"
            and a.media_type
            and "geopackage" in a.media_type,
            "flowfiles": lambda k, a: k.endswith("-flowfile"),
        },
    ),
}


@dataclass(order=True)
class Interval:
    start: datetime
    end: datetime
    assets: Dict[str, List[str]] = field(default_factory=lambda: defaultdict(list))


def format_results(item_iter: Any) -> Dict[str, Dict[str, Dict[str, List[str]]]]:
    """
    Main grouping:
      - item‐level grouping for GFM/HWM
      - asset‐level grouping for BLE/NWS/USGS/Ripple
      - fallback by file‐type
    Returns a nested dict:
      {coll_short → group_id → {extents: [...], flowfiles: [...]} }
    """
    results = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    seen = set()
    ripple_cache: Dict[str, List[str]] = {}
    ripple_best_items: Dict[Any, Tuple[str, str]] = {}

    for idx, item in enumerate(item_iter, 1):
        coll = item.collection_id or "<none>"
        # ripple version‐filtering
        if coll == "ripple-fim-collection":
            src = item.properties.get("source", "")
            hucs = tuple(item.properties.get("hucs", []))
            ver = item.properties.get("flows2fim_version", "")
            if src and hucs and ver:
                key = (src, hucs)
                prev = ripple_best_items.get(key)
                if not prev or compare_versions(ver, prev[1]) > 0:
                    ripple_best_items[key] = (item.id, ver)
                else:
                    logger.info("Skipping %s (ripple newer exists)", item.id)
                    continue

        if item.id in seen:
            continue
        seen.add(item.id)

        short = coll.replace("-collection", "").replace("-fim", "")
        if short == "gfm-expanded":
            short = "gfm_expanded"

        # 1) item‐level mapping
        if coll in COLLECTIONS:
            group_fn, tests = COLLECTIONS[coll]
            gid = group_fn(item)
            bucket = results[short][gid]
            for key, asset in item.assets.items():
                if not asset.href:
                    continue
                for atype, test in tests.items():
                    if test(key, asset) and asset.href not in bucket[atype]:
                        bucket[atype].append(asset.href)

        # 2) BLE/NWS/USGS/Ripple
        elif coll == "ble-collection" or coll.endswith("-fim-collection"):
            # build ripple cache once
            if coll == "ripple-fim-collection" and not ripple_cache:
                col = item.get_collection()
                for ak, aa in col.assets.items():
                    m = re.search(r"flows_(\d+)_yr_", ak)
                    ri = f"{m.group(1)}yr" if m else None
                    if ri and aa.media_type == "text/csv":
                        ripple_cache.setdefault(ri, []).append(aa.href)

            specs = (
                BLE_SPEC
                if coll == "ble-collection"
                else RIPPLE_SPEC if coll == "ripple-fim-collection" else NWS_USGS_SPEC
            )
            found = set()
            for key, asset in item.assets.items():
                if not asset.href:
                    continue
                for pat, gid_t, at_t in specs:
                    m = pat.match(key)
                    if not m:
                        continue
                    gid = m.expand(gid_t) if "\\" in gid_t else gid_t
                    at = m.expand(at_t)
                    bucket = results[short][gid]
                    if asset.href not in bucket[at]:
                        bucket[at].append(asset.href)
                    found.add(gid)
            # attach cached ripple flowfiles
            if coll == "ripple-fim-collection":
                for gid in found:
                    for href in ripple_cache.get(gid, []):
                        if href not in results[short][gid]["flowfiles"]:
                            results[short][gid]["flowfiles"].append(href)

        # 3) fallback by media_type
        else:
            logger.warning("Unknown coll %r → fallback grouping", coll)
            short = short or item.id
            bucket = results[short][item.id]
            for key, asset in item.assets.items():
                if not asset.href:
                    continue
                mt = (asset.media_type or "").lower()
                if "tiff" in mt and "extent" in key:
                    bucket["extents"].append(asset.href)
                elif "csv" in mt and "flow" in key:
                    bucket["flowfiles"].append(asset.href)

    logger.info("format_results: grouped %d items", len(seen))
    return results


def merge_gfm_expanded(
    groups: Dict[str, Dict[str, List[str]]],
    tolerance_days: int = 3,
) -> Dict[str, Dict[str, List[str]]]:
    """
    Merge adjacent time‐interval groups within tolerance_days. Set tolerancce days for observation merging to 3 days because that is the outer limit for how long it should take the two sentinal 1 satellite's swaths to cover the continental US when operating under normal conditions and creating flood maps from both the ascending and descending orbits (which is what GFM does). Even if an event continues past that 3 days would still probably want to subset as another FIM grouping since the flow conditions will likely have changed dramatically by then.
    Input: { "2020-01-01T00:00:00Z/2020-01-05T00:00:00Z": {...}, … }
    Output: same shape but fewer ranges.
    """
    if not groups:
        return {}

    tol = timedelta(days=tolerance_days)
    intervals: List[Interval] = []
    for key, assets in groups.items():
        try:
            start_s, end_s = key.split("/")
            iv = Interval(start=parse_iso(start_s), end=parse_iso(end_s))
            for at, hrefs in assets.items():
                iv.assets[at].extend(hrefs)
            intervals.append(iv)
        except Exception as e:
            logger.warning("Bad gfm-expanded key %r: %s", key, e)

    intervals.sort()
    merged: List[Interval] = []
    cur = intervals[0]
    for nxt in intervals[1:]:
        if nxt.start <= cur.end + tol:
            cur.end = max(cur.end, nxt.end)
            for at, hrefs in nxt.assets.items():
                for h in hrefs:
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
