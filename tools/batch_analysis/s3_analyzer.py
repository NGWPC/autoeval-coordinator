"""S3 metrics analysis for pipeline batch runs."""

import logging
from typing import Dict, List
import os

import fsspec
import boto3
from botocore.config import Config

from .models import DebugConfig

logger = logging.getLogger(__name__)


class S3MetricsAnalyzer:
    """Analyzes S3 output directories for missing or invalid metrics files."""

    def __init__(self, config: DebugConfig):
        self.config = config
        if config.s3_output_root:
            # Use the default profile from .aws/credentials
            # This will override environment variables
            self.fs = fsspec.filesystem(
                "s3",
                profile="default"
            )

    def find_missing_metrics(self) -> List[Dict[str, str]]:
        """Find directories missing metrics.csv files."""
        if not self.config.s3_output_root:
            logger.warning("No S3 output root provided, skipping metrics analysis")
            return []

        logger.info("Scanning S3 for missing metrics files...")

        try:
            # Get all files in the batch directory
            batch_path = f"{self.config.s3_output_root.rstrip('/')}"
            if not batch_path.startswith("s3://"):
                batch_path = f"s3://{batch_path}"

            # Remove s3:// prefix for fsspec
            s3_path = batch_path[5:]

            all_files = self.fs.glob(f"{s3_path}/**/*")

            # Track directories and which have metrics files
            directories = set()
            has_metrics = set()

            for file_path in all_files:
                # Skip agg_metrics.csv and catchment-data-indices directories
                if (
                    "agg_metrics.csv" in file_path
                    or "catchment-data-indices" in file_path
                    or "catchment-extents" in file_path
                ):
                    continue

                parts = file_path.split("/")
                if len(parts) > 4:  # Ensure we have a nested structure
                    # Get directory path (excluding filename)
                    dir_path = "/".join(parts[:-1]) + "/"
                    directories.add(dir_path)

                    # Check if this is a metrics.csv file
                    if file_path.endswith("__metrics.csv"):
                        has_metrics.add(dir_path)

            # Find directories without metrics
            missing_metrics = []
            for directory in directories:
                if directory not in has_metrics:
                    # Only report directories that are nested enough
                    if len(directory.split("/")) > 6:  # Adjust based on your structure
                        missing_metrics.append({"directory": f"s3://{directory}", "issue": "Missing metrics.csv file"})

            logger.info(f"Found {len(missing_metrics)} directories missing metrics files")
            return missing_metrics

        except Exception as e:
            logger.error(f"Error scanning S3 for missing metrics: {e}")
            return []

    def find_empty_metrics(self) -> List[Dict[str, str]]:
        """Find metrics.csv files that are empty or have incorrect row counts."""
        if not self.config.s3_output_root:
            return []

        logger.info("Checking metrics files for correct content...")

        try:
            batch_path = f"{self.config.s3_output_root.rstrip('/')}"
            if not batch_path.startswith("s3://"):
                batch_path = f"s3://{batch_path}"

            s3_path = batch_path[5:]
            metrics_files = self.fs.glob(f"{s3_path}/**/*__metrics.csv")

            empty_metrics = []
            for metrics_file in metrics_files:
                # Skip agg_metrics files
                if "agg_metrics.csv" in metrics_file:
                    continue

                try:
                    with self.fs.open(f"s3://{metrics_file}", "r") as f:
                        lines = f.readlines()
                        line_count = len(lines)

                    # Expected: header + 1 data row = 2 lines
                    if line_count != 2:
                        empty_metrics.append(
                            {
                                "file": f"s3://{metrics_file}",
                                "line_count": str(line_count),
                                "issue": f"Incorrect row count (expected 2, got {line_count})",
                            }
                        )

                except Exception as e:
                    empty_metrics.append(
                        {
                            "file": f"s3://{metrics_file}",
                            "line_count": "Error",
                            "issue": f"Error reading file: {str(e)}",
                        }
                    )

            logger.info(f"Found {len(empty_metrics)} metrics files with issues")
            return empty_metrics

        except Exception as e:
            logger.error(f"Error checking metrics file content: {e}")
            return []

    def find_missing_agg_metrics(self) -> List[Dict[str, str]]:
        """Find directories missing agg_metrics.csv files."""
        if not self.config.s3_output_root:
            return []

        logger.info("Checking for missing agg_metrics.csv files...")

        try:
            batch_path = f"{self.config.s3_output_root.rstrip('/')}"
            if not batch_path.startswith("s3://"):
                batch_path = f"s3://{batch_path}"

            s3_path = batch_path[5:]

            # Find all directories that should have agg_metrics.csv (typically HUC-level directories)
            all_items = self.fs.ls(s3_path)
            directories = [item for item in all_items if self.fs.isdir(item)]

            missing_agg = []
            for directory in directories:
                # Skip certain directories
                if any(skip in directory for skip in ["stac_aois", "logs"]):
                    continue

                expected_agg_file = f"{directory}/agg_metrics.csv"
                if not self.fs.exists(expected_agg_file):
                    # Try to find what the actual filename should be
                    dir_name = directory.split("/")[-1]
                    expected_agg_file_alt = f"{directory}/{dir_name}__agg_metrics.csv"

                    if not self.fs.exists(expected_agg_file_alt):
                        missing_agg.append(
                            {
                                "directory": f"s3://{directory}",
                                "expected_file": f"s3://{expected_agg_file_alt}",
                                "issue": "Missing agg_metrics.csv file",
                            }
                        )

            logger.info(f"Found {len(missing_agg)} directories missing agg_metrics files")
            return missing_agg

        except Exception as e:
            logger.error(f"Error checking for missing agg_metrics: {e}")
            return []