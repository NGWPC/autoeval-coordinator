import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import fsspec
import pandas as pd

logger = logging.getLogger(__name__)


class MetricsAggregator:
    """Aggregates metrics.csv files from different scenarios into a single results.csv file."""

    def __init__(self, outputs_path: str, stac_results: Dict[str, Dict[str, Dict[str, List[str]]]], data_service):
        """
        Initialize the MetricsAggregator.

        Args:
            outputs_path: Path to the HUC output directory (e.g., "/top-level/HAND-ver4/HUC-11090202/")
            stac_results: Results from StacQuerier.query_stac_for_polygon containing STAC item IDs
            data_service: DataService instance for file operations
        """
        self.outputs_path = outputs_path
        self.stac_results = stac_results
        self.data_service = data_service

    def aggregate_metrics(self) -> pd.DataFrame:
        """
        Aggregate all metrics.csv files into a single DataFrame.

        Returns:
            DataFrame with columns: collection_name, stac_item_id, scenario, and all metrics columns
        """
        all_metrics = []

        # Find all metrics.csv files in the directory structure
        metrics_files = self.data_service.find_metrics_files(self.outputs_path)

        if not metrics_files:
            logger.warning(f"No metrics.csv files found in {self.outputs_path}")
            return pd.DataFrame()

        logger.info(f"Found {len(metrics_files)} metrics.csv files to aggregate")

        for metrics_file in metrics_files:
            try:
                # Extract collection name and scenario from path
                # Path structure: base/COLLECTION/SCENARIO/metrics.csv
                if metrics_file.startswith("s3://"):
                    # For S3 paths, split by '/' and get the last components
                    path_parts = metrics_file.rstrip("/").split("/")
                    scenario_name = path_parts[-2]  # Second to last part
                    collection_name = path_parts[-3]  # Third to last part
                else:
                    # For local paths, use Path
                    path_obj = Path(metrics_file)
                    scenario_name = path_obj.parent.name
                    collection_name = path_obj.parent.parent.name

                # Read the metrics CSV using fsspec.open (works for both local and S3)
                with fsspec.open(
                    metrics_file, **self.data_service._s3_options if metrics_file.startswith("s3://") else {}
                ) as f:
                    metrics_df = pd.read_csv(f, index_col=0)

                if metrics_df.empty:
                    logger.warning(f"Empty metrics file: {metrics_file}")
                    continue

                stac_items = self._get_stac_items_for_scenario(collection_name, scenario_name)

                # Add the required columns to each row
                metrics_df["collection_name"] = collection_name
                metrics_df["stac_item_id"] = [";".join(stac_items)] * len(
                    metrics_df
                )  # Join STAC items into semicolon-separated string
                metrics_df["scenario"] = scenario_name

                all_metrics.append(metrics_df)

                logger.info(f"Processed metrics for {collection_name}/{scenario_name}: {len(metrics_df)} rows")

            except Exception as e:
                logger.error(f"Error processing metrics file {metrics_file}: {e}")
                continue

        if not all_metrics:
            logger.warning("No valid metrics files were processed")
            return pd.DataFrame()

        # Combine all metrics into a single DataFrame
        combined_df = pd.concat(all_metrics, ignore_index=True)

        # Reorder columns to put metadata first
        metadata_cols = ["collection_name", "stac_item_id", "scenario"]
        metrics_cols = [col for col in combined_df.columns if col not in metadata_cols]
        combined_df = combined_df[metadata_cols + metrics_cols]

        logger.info(f"Aggregated {len(combined_df)} total metric rows from {len(all_metrics)} scenarios")

        return combined_df

    def _get_stac_items_for_scenario(self, collection_name: str, scenario_name: str) -> List[str]:
        """
        Get STAC item IDs for a specific collection and scenario.

        Args:
            collection_name: Name of the collection (e.g., "BLE", "USGS")
            scenario_name: Name of the scenario (e.g., "100yr", "Minor")

        Returns:
            List of STAC item IDs for this scenario
        """
        # Just ensure lowercase for consistency
        stac_collection = collection_name.lower()

        # Look for the scenario in the STAC results
        if stac_collection in self.stac_results:
            # For BLE: scenario names like "100yr"
            # For NWS/USGS: scenario names like "action", "minor", "moderate", "major"
            scenarios = self.stac_results[stac_collection]

            # Try exact match first
            if scenario_name in scenarios:
                return scenarios[scenario_name].get("stac_items", [])

            # Try case-insensitive match
            for scenario_key, scenario_data in scenarios.items():
                if scenario_key.lower() == scenario_name.lower():
                    return scenario_data.get("stac_items", [])

        logger.warning(f"No STAC items found for {collection_name}/{scenario_name}")
        return []

    def save_results(self, output_path: str) -> str:
        """
        Aggregate metrics and save to results.csv.

        Args:
            output_path: Path where results.csv should be saved (required)

        Returns:
            Path to the saved results.csv file
        """

        # Aggregate the metrics
        results_df = self.aggregate_metrics()

        if results_df.empty:
            logger.warning("No metrics to aggregate, creating empty results.csv")
            results_df = pd.DataFrame(columns=["collection_name", "stac_item_id", "scenario"])

        # Save to CSV using fsspec (works for both local and S3)
        with fsspec.open(
            output_path, "w", **self.data_service._s3_options if output_path.startswith("s3://") else {}
        ) as f:
            results_df.to_csv(f, index=False)

        logger.info(f"Results saved to {output_path}")
        return output_path
