"""
Flowfile Combiner - A class-based interface for combining flowfiles
from STAC query results into scenario-specific combined CSV files.
"""

import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional

import fsspec
import pandas as pd

logger = logging.getLogger(__name__)


class FlowfileCombiner:
    """
    A class for combining multiple flowfiles into single CSV files.
    Provides deduplication and scenario-based organization.
    """

    def __init__(self, output_dir: str = "combined_flowfiles"):
        """
        Initialize the FlowfileCombiner.

        Args:
            output_dir: Directory to save combined flowfiles
        """
        self.output_dir = output_dir
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

    def combine_flowfiles(self, flowfile_paths: List[str], output_path: str) -> str:
        """
        Combine multiple flowfiles into a single CSV file.

        Args:
            flowfile_paths: List of S3 or local paths to flowfiles
            output_path: Path where combined flowfile will be saved

        Returns:
            Path to the combined flowfile

        Raises:
            ValueError: If no flowfiles could be read or combined data is invalid
        """
        all_dfs = []
        header = None

        logger.info(f"Combining {len(flowfile_paths)} flowfiles")
        for flowfile_path in flowfile_paths:
            try:
                logger.debug(f"  Reading: {flowfile_path}")
                with fsspec.open(flowfile_path, "r") as f:
                    df = pd.read_csv(f)
                    if header is None:
                        header = df.columns.tolist()
                    all_dfs.append(df)
            except Exception as e:
                logger.warning(f"  Could not read {flowfile_path}: {e}")
                continue

        if not all_dfs:
            raise ValueError("No flowfiles could be read")

        combined_df = pd.concat(all_dfs, ignore_index=True)

        if len(combined_df.columns) < 2:
            raise ValueError(f"Combined flowfile must have at least 2 columns.")

        # Deduplicate rows based on the first column, keeping the maximum value in the second column
        combined_df = combined_df.groupby(combined_df.columns[0], as_index=False)[combined_df.columns[1]].max()

        # Ensure output directory exists
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        combined_df.to_csv(output_path, index=False, header=header)
        logger.info(f"  Combined flowfile saved to: {output_path} ({len(combined_df)} rows)")
        
        return output_path

    def process_stac_query_results(
        self, 
        stac_results: Dict, 
        temp_dir: Optional[str] = None
    ) -> Dict[str, Dict[str, str]]:
        """
        Process STAC query results and combine flowfiles for all scenarios/events.

        Args:
            stac_results: Dictionary from STAC query results
            temp_dir: Optional temporary directory for intermediate files

        Returns:
            Dictionary mapping collection -> scenario -> combined flowfile path
        """
        if temp_dir is None:
            temp_dir = self.output_dir

        results = {}

        for collection_name, scenarios in stac_results.items():
            logger.info(f"Processing collection: {collection_name}")
            results[collection_name] = {}

            for scenario_name, scenario_data in scenarios.items():
                logger.info(f"  Processing scenario: {scenario_name}")

                # Find flow key (flowfiles, flow_file, etc.)
                flow_key = None
                for key in scenario_data.keys():
                    if "flow" in key.lower():
                        flow_key = key
                        break

                if not flow_key:
                    logger.info(f"    No flow key found in {scenario_name}")
                    continue

                flowfile_paths = scenario_data[flow_key]
                if not flowfile_paths:
                    logger.info(f"    No flowfiles to combine for {scenario_name}")
                    continue

                collection_output_dir = os.path.join(temp_dir, collection_name)
                os.makedirs(collection_output_dir, exist_ok=True)

                # Create safe filename
                safe_scenario_name = "".join(c for c in scenario_name if c.isalnum() or c in ('-', '_')).rstrip()
                output_filename = f"{collection_name}_{safe_scenario_name}_combined_flowfile.csv"
                output_path = os.path.join(collection_output_dir, output_filename)

                try:
                    self.combine_flowfiles(flowfile_paths, output_path)
                    results[collection_name][scenario_name] = output_path
                except Exception as e:
                    logger.error(f"    Error combining flowfiles for {scenario_name}: {e}")
                    continue

        return results

    def process_stac_json_file(self, stac_json_path: str) -> Dict[str, Dict[str, str]]:
        """
        Process STAC query results from a JSON file.

        Args:
            stac_json_path: Path to the STAC query results JSON file

        Returns:
            Dictionary mapping collection -> scenario -> combined flowfile path
        """
        logger.info(f"Loading STAC query results from: {stac_json_path}")
        with fsspec.open(stac_json_path, "r") as f:
            stac_data = json.load(f)

        return self.process_stac_query_results(stac_data)

    def get_scenario_count(self, stac_results: Dict) -> int:
        """
        Count total number of scenarios across all collections.

        Args:
            stac_results: Dictionary from STAC query results

        Returns:
            Total number of scenarios
        """
        total = 0
        for collection_name, scenarios in stac_results.items():
            for scenario_name, scenario_data in scenarios.items():
                # Check if scenario has flowfiles
                flow_key = None
                for key in scenario_data.keys():
                    if "flow" in key.lower():
                        flow_key = key
                        break
                
                if flow_key and scenario_data[flow_key]:
                    total += 1
        
        return total