import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import fsspec
import pandas as pd

from pipeline_utils import PathFactory

logger = logging.getLogger(__name__)


class MetricsAggregator:
    """Aggregates metrics.csv files from different scenarios into a single agg_metrics.csv file."""

    def __init__(
        self,
        outputs_path: str,
        stac_results: Dict[str, Dict[str, Dict[str, List[str]]]],
        data_service,
        flow_scenarios: Optional[Dict[str, Dict[str, str]]] = None,
        aoi_name: Optional[str] = None,
    ):
        """
        Initialize the MetricsAggregator.

        Args:
            outputs_path: Path to the HUC output directory (e.g., "/top-level/HAND-ver4/HUC-11090202/")
            stac_results: Results from StacQuerier.query_stac_for_polygon containing STAC item IDs
            data_service: DataService instance for file operations
            flow_scenarios: Mapping of collection -> scenario -> flowfile path
            aoi_name: AOI name for generating correct S3 flowfile paths
        """
        self.outputs_path = outputs_path
        self.stac_results = stac_results
        self.data_service = data_service
        self.flow_scenarios = flow_scenarios or {}

        # Create PathFactory to generate correct S3 flowfile paths
        if aoi_name:
            self.path_factory = PathFactory(None, outputs_path, aoi_name)
        else:
            self.path_factory = None

    def aggregate_metrics(self) -> pd.DataFrame:
        """
        Aggregate all metrics.csv files into a single DataFrame.

        Returns:
            DataFrame with columns: collection_id, stac_item_id, scenario, and all metrics columns
        """
        # Map collection names from directory structure to their full STAC collection IDs
        collection_mapping = {
            "ble": "ble-collection",
            "nws": "nws-fim-collection",
            "usgs": "usgs-fim-collection",
            "ripple": "ripple-fim-collection",
            "gfm": "gfm-collection",
            "gfm_expanded": "gfm_expanded",  # Special case
            "hwm": "hwm-collection",
        }

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

                stac_items, gauge = self._get_stac_items_for_scenario(
                    collection_name, scenario_name, collection_mapping
                )

                # Add the required columns to each row
                # Use the full collection ID from the mapping
                full_collection_id = collection_mapping.get(collection_name.lower(), collection_name)
                metrics_df["collection_id"] = full_collection_id
                metrics_df["stac_item_id"] = [";".join(stac_items)] * len(
                    metrics_df
                )  # Join STAC items into semicolon-separated string
                metrics_df["scenario"] = scenario_name

                # Add flow column with flowfile S3 URI
                if self.path_factory:
                    # Generate the correct S3 path using PathFactory
                    flow_uri = self.path_factory.flowfile_path(collection_name, scenario_name)
                else:
                    # Fall back to the original flow_scenarios mapping (temp paths)
                    flow_uri = self.flow_scenarios.get(collection_name, {}).get(scenario_name, "")
                metrics_df["flow"] = flow_uri

                # Add nws_lid column with gauge information
                metrics_df["nws_lid"] = gauge

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
        metadata_cols = ["collection_id", "stac_item_id", "scenario", "flow", "nws_lid"]
        metrics_cols = [col for col in combined_df.columns if col not in metadata_cols]
        combined_df = combined_df[metadata_cols + metrics_cols]

        logger.info(f"Aggregated {len(combined_df)} total metric rows from {len(all_metrics)} scenarios")

        return combined_df

    def _get_stac_items_for_scenario(
        self, collection_name: str, scenario_name: str, collection_mapping: dict
    ) -> tuple[List[str], Optional[str]]:
        """
        Get STAC item IDs and gauge information for a specific collection and scenario.

        Args:
            collection_name: Name of the collection (e.g., "BLE", "USGS")
            scenario_name: Name of the scenario (e.g., "100yr", "Minor")
            collection_mapping: Mapping from short names to full collection IDs

        Returns:
            Tuple of (List of STAC item IDs for this scenario, gauge string or None)
        """
        # Get the full collection ID from the mapping
        stac_collection = collection_mapping.get(collection_name.lower(), collection_name.lower())

        # Look for the scenario in the STAC results
        if stac_collection in self.stac_results:
            # For BLE: scenario names like "100yr"
            # For NWS/USGS: scenario names like "action", "minor", "moderate", "major"
            scenarios = self.stac_results[stac_collection]

            # Try exact match first
            if scenario_name in scenarios:
                scenario_data = scenarios[scenario_name]
                return scenario_data.get("stac_items", []), scenario_data.get("gauge")

            # Try case-insensitive match
            for scenario_key, scenario_data in scenarios.items():
                if scenario_key.lower() == scenario_name.lower():
                    return scenario_data.get("stac_items", []), scenario_data.get("gauge")

        logger.warning(f"No STAC items found for {collection_name}/{scenario_name}")
        return [], None

    def save_results(self, output_path: str) -> str:
        """
        Aggregate metrics and upsert into existing agg_metrics.csv.
        Uses metrics columns to identify and replace identical rows.

        Args:
            output_path: Path where agg_metrics.csv should be saved (required)

        Returns:
            Path to the saved agg_metrics.csv file
        """
        # Aggregate the metrics
        new_df = self.aggregate_metrics()

        if new_df.empty:
            logger.warning("No metrics to aggregate")
            new_df = pd.DataFrame(columns=["collection_id", "stac_item_id", "scenario", "flow", "nws_lid"])

        # Try to read existing agg_metrics file
        try:
            with fsspec.open(
                output_path, "r", **self.data_service._s3_options if output_path.startswith("s3://") else {}
            ) as f:
                existing_df = pd.read_csv(f)
                logger.info(f"Found existing agg_metrics.csv with {len(existing_df)} rows")
        except (FileNotFoundError, pd.errors.EmptyDataError):
            existing_df = pd.DataFrame()
            logger.info("No existing agg_metrics.csv found, creating new file")
        except Exception as e:
            existing_df = pd.DataFrame()
            logger.warning(f"Error reading existing agg_metrics.csv: {e}, creating new file")

        # Shortcut cases
        if existing_df.empty:
            final_df = new_df
        elif new_df.empty:
            final_df = existing_df
        else:
            final_df = self._merge_dataframes(existing_df, new_df)

        # Ensure consistent column order
        if not final_df.empty:
            metadata_cols = ["collection_id", "stac_item_id", "scenario", "flow", "nws_lid"]
            existing_metadata_cols = [col for col in metadata_cols if col in final_df.columns]
            other_cols = [col for col in final_df.columns if col not in metadata_cols]
            final_df = final_df[existing_metadata_cols + other_cols]

        # Save to CSV using fsspec (works for both local and S3)
        with fsspec.open(
            output_path, "w", **self.data_service._s3_options if output_path.startswith("s3://") else {}
        ) as f:
            final_df.to_csv(f, index=False)

        new_rows = len(new_df) if existing_df.empty else len(final_df) - len(existing_df) + len(new_df)
        logger.info(f"Results saved to {output_path} with {len(final_df)} total rows ({len(new_df)} new)")
        return output_path

    def _merge_dataframes(self, existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
        """
        Merge existing and new dataframes using vectorized operations.
        For rows with nearly identical metrics, keep the one with more complete metadata.
        """
        # Define metadata columns
        meta_cols = ["collection_id", "stac_item_id", "scenario", "flow", "nws_lid"]

        # Auto-detect metrics columns (numeric columns not in metadata)
        metrics_cols = [
            col for col in new_df.columns if col not in meta_cols and pd.api.types.is_numeric_dtype(new_df[col])
        ]

        if not metrics_cols:
            logger.warning("No numeric metrics columns found, appending all new data")
            return pd.concat([existing_df, new_df], ignore_index=True)

        # Tag source and compute metadata completeness score
        existing_df = existing_df.copy()
        existing_df["_source"] = "existing"
        new_df = new_df.copy()
        new_df["_source"] = "new"

        combined = pd.concat([existing_df, new_df], ignore_index=True)

        # Define the precision for rounding floating-point metrics
        rounding_precision = 5

        # Create temporary columns with rounded metrics for duplicate detection
        rounded_metric_cols = [f"{col}_rounded" for col in metrics_cols]
        for i, col in enumerate(metrics_cols):
            combined[rounded_metric_cols[i]] = combined[col].round(rounding_precision)

        # Count non-null, non-empty metadata for each row
        combined["_meta_count"] = combined[meta_cols].notna().sum(axis=1) - (combined[meta_cols] == "").sum(axis=1)

        # Prefer new on ties
        combined["_is_new"] = (combined["_source"] == "new").astype(int)

        # Sort so best row per metrics key comes first, using the rounded metrics for sorting
        sort_cols = rounded_metric_cols + ["_meta_count", "_is_new"]
        ascending = [True] * len(rounded_metric_cols) + [False, False]
        combined = combined.sort_values(by=sort_cols, ascending=ascending)

        # Drop duplicates on key metadata + rounded metrics columns, keeping the first (best) row
        key_cols = ["collection_id", "scenario"] + rounded_metric_cols
        final_df = combined.drop_duplicates(subset=key_cols, keep="first")

        # Clean up temporary columns
        final_df = final_df.drop(columns=["_source", "_meta_count", "_is_new"] + rounded_metric_cols)

        rows_removed = len(existing_df) + len(new_df) - len(final_df)
        logger.info(f"Merged dataframes: removed {rows_removed} duplicate rows based on metrics")

        return final_df
