import argparse
import logging
import sys
from pathlib import Path
from typing import List, Optional

import fsspec
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_stac_item_directories(output_root: str) -> List[tuple[str, str]]:
    """
    Get all STAC item subdirectories from the output root.

    Returns:
        List of tuples (stac_item_code, full_path)
    """
    stac_item_dirs = []

    if output_root.startswith("s3://"):
        fs = fsspec.filesystem("s3", profile="fimc-data")
        try:
            items = fs.ls(output_root, detail=True)
            for item in items:
                if item["type"] == "directory":
                    stac_item_code = item["name"].split("/")[-1]
                    full_path = item["name"]
                    if not full_path.endswith("/"):
                        full_path += "/"
                    stac_item_dirs.append((stac_item_code, full_path))
        except Exception as e:
            logger.error(f"Error listing S3 directories: {e}")
    else:
        # Local filesystem
        output_path = Path(output_root)
        if output_path.exists() and output_path.is_dir():
            for item in output_path.iterdir():
                if item.is_dir():
                    stac_item_dirs.append((item.name, str(item)))

    return stac_item_dirs


def read_agg_metrics(agg_metrics_path: str) -> Optional[pd.DataFrame]:
    """
    Read an agg_metrics.csv file.

    Args:
        agg_metrics_path: Path to the agg_metrics.csv file

    Returns:
        DataFrame or None if file doesn't exist or can't be read
    """
    try:
        with fsspec.open(agg_metrics_path, "r") as f:
            df = pd.read_csv(f)
            return df
    except FileNotFoundError:
        logger.warning(f"File not found: {agg_metrics_path}")
        return None
    except Exception as e:
        logger.error(f"Error reading {agg_metrics_path}: {e}")
        return None


def aggregate_metrics(output_root: str, calb: bool, hand_version: str, resolution: str) -> pd.DataFrame:
    """
    Aggregate all agg_metrics.csv files from HUC subdirectories.

    Args:
        output_root: Root directory containing HUC subdirectories
        calb: Calibration flag (True/False)
        hand_version: HAND version value
        resolution: Resolution in meters

    Returns:
        Combined DataFrame with all metrics
    """
    all_metrics = []

    stac_item_dirs = get_stac_item_directories(output_root)

    if not stac_item_dirs:
        logger.warning(f"No subdirectories found in {output_root}")
        return pd.DataFrame()

    logger.info(f"Found {len(stac_item_dirs)} STAC item directories")

    for stac_item_code, stac_item_path in stac_item_dirs:
        if stac_item_path.startswith("s3://"):
            agg_metrics_path = f"{stac_item_path.rstrip('/')}/{stac_item_code}__agg_metrics.csv"
        elif output_root.startswith("s3://"):
            # Handle case where stac_item_path doesn't have s3:// prefix but output_root does
            agg_metrics_path = f"s3://{stac_item_path.rstrip('/')}/{stac_item_code}__agg_metrics.csv"
        else:
            agg_metrics_path = str(Path(stac_item_path) / f"{stac_item_code}__agg_metrics.csv")

        df = read_agg_metrics(agg_metrics_path)

        if df is not None and not df.empty:
            # Add new columns
            df["calibrated"] = "True" if calb else "False"
            df["version"] = hand_version
            df["resolution_m"] = resolution
            df["extent_config"] = "COMP"
            df["full_json_path"] = "null"

            all_metrics.append(df)
            logger.info(f"Processed {len(df)} rows from STAC item {stac_item_code}")
        else:
            logger.warning(f"No valid data found for STAC item {stac_item_code}")

    if not all_metrics:
        logger.warning("No valid agg_metrics.csv files were found")
        return pd.DataFrame()

    # Combine all DataFrames
    combined_df = pd.concat(all_metrics, ignore_index=True)
    logger.info(f"Combined {len(combined_df)} total rows from {len(all_metrics)} STAC items")

    return combined_df


def main():
    parser = argparse.ArgumentParser(
        description="Aggregate agg_metrics.csv files from multiple STAC items into a master_metrics.csv file"
    )

    parser.add_argument("output_root", help="Root directory containing STAC item subdirectories (can be S3 path)")

    parser.add_argument("--calb", action="store_true", help="Set calibration flag to True (default: False)")

    parser.add_argument("--hand-version", required=True, help="HAND version value to add to all rows")

    parser.add_argument("--resolution", required=True, help="Resolution in meters to add to all rows")

    args = parser.parse_args()

    # Clean up output root path
    output_root = args.output_root.rstrip("/")

    logger.info(f"Starting aggregation from {output_root}")
    logger.info(f"Parameters: calb={args.calb}, " f"hand_version={args.hand_version}, resolution={args.resolution}")

    master_df = aggregate_metrics(output_root, args.calb, args.hand_version, args.resolution)

    if master_df.empty:
        logger.error("No data to write to master_metrics.csv")
        sys.exit(1)

    if output_root.startswith("s3://"):
        master_metrics_path = f"{output_root}/master_metrics.csv"
    else:
        master_metrics_path = str(Path(output_root) / "master_metrics.csv")

    try:
        with fsspec.open(master_metrics_path, "w") as f:
            master_df.to_csv(f, index=False)
        logger.info(f"Successfully wrote {len(master_df)} rows to {master_metrics_path}")
    except Exception as e:
        logger.error(f"Error writing master_metrics.csv: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
