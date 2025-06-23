#!/usr/bin/env python3
import argparse
import json
import os
import shutil
import tempfile

import fsspec
import pandas as pd


def run_test_mode():
    """
    Run combine_flowfiles in test mode with synthetic data.

    Creates temporary test files and demonstrates the flowfile merging.
    All output is printed to stdout and temporary files are cleaned up.
    """

    temp_dir = tempfile.mkdtemp()

    try:
        # Test data: 10 rows each, with 3 overlapping IDs (id002, id005, id008)
        test_data = [
            # File 1: id001-id010 with base discharge values
            pd.DataFrame(
                {"featureid": [f"id{i:03d}" for i in range(1, 11)], "discharge": [100.5 + i * 25.3 for i in range(10)]}
            ),
            # File 2: id002,id005,id008 (overlaps) + id011-id017 with different values
            pd.DataFrame(
                {
                    "featureid": ["id002", "id005", "id008"] + [f"id{i:03d}" for i in range(11, 18)],
                    "discharge": [250.1, 135.2, 225.8] + [110.5 + i * 15.4 for i in range(7)],
                }
            ),
        ]

        temp_paths = []
        for i, df in enumerate(test_data, 1):
            print(f"\n=== Test Flowfile {i} ===")
            csv_content = df.to_csv(index=False)
            print(csv_content)

            temp_path = os.path.join(temp_dir, f"test_flowfile{i}.csv")
            df.to_csv(temp_path, index=False)
            temp_paths.append(temp_path)

        output_path = os.path.join(temp_dir, "combined_output.csv")

        combine_flowfiles(temp_paths, output_path)

        # Read and print the combined result
        print(f"\n=== Combined Flowfile (test_collection/test_scenario) ===")
        with open(output_path, "r") as f:
            combined_content = f.read()
            print(combined_content)

        # Show deduplication summary
        combined_df = pd.read_csv(output_path)
        overlap_ids = ["id002", "id005", "id008"]
        print("\n=== Deduplication Summary ===")
        for feature_id in overlap_ids:
            values = []
            for i, df in enumerate(test_data, 1):
                if feature_id in df["featureid"].values:
                    val = df[df["featureid"] == feature_id]["discharge"].values[0]
                    values.append(f"File {i}: {val}")
            final_val = combined_df[combined_df["featureid"] == feature_id]["discharge"].values[0]
            print(f"{feature_id}: {' vs '.join(values)} → Kept: {final_val} (max)")

        print("\nTest completed successfully!")

    finally:
        shutil.rmtree(temp_dir)


def combine_flowfiles(flowfile_paths, output_path):
    """
    Combine multiple flowfiles into a single CSV file.

    Args:
        flowfile_paths: List of S3 or local paths to flowfiles
        output_path: Path where combined flowfile will be saved
    """
    all_dfs = []
    header = None

    for flowfile_path in flowfile_paths:
        try:
            print(f"  Reading: {flowfile_path}")
            with fsspec.open(flowfile_path, "r") as f:
                df = pd.read_csv(f)
                if header is None:
                    header = df.columns.tolist()
                all_dfs.append(df)
        except Exception as e:
            print(f"  Warning: Could not read {flowfile_path}: {e}")
            continue

    if not all_dfs:
        raise ValueError("No flowfiles could be read")

    combined_df = pd.concat(all_dfs, ignore_index=True)

    # Deduplicate rows based on the first column, keeping the maximum value in the second column
    if len(combined_df.columns) >= 2:
        combined_df = combined_df.groupby(combined_df.columns[0], as_index=False)[combined_df.columns[1]].max()

    combined_df.to_csv(output_path, index=False, header=header)
    print(f"  Combined flowfile saved to: {output_path}")


def process_stac_query_results(stac_json_path, output_dir="combined_flowfiles"):
    """
    Process STAC query results and combine flowfiles for all scenarios/events.

    Args:
        stac_json_path: Path to the STAC query results JSON file
        output_dir: Directory to save combined flowfiles

    Returns:
        Dictionary mapping collection -> scenario -> combined flowfile path
    """
    print(f"Loading STAC query results from: {stac_json_path}")
    with fsspec.open(stac_json_path, "r") as f:
        stac_data = json.load(f)

    results = {}

    for collection_name, scenarios in stac_data.items():
        print(f"\nProcessing collection: {collection_name}")
        results[collection_name] = {}

        for scenario_name, scenario_data in scenarios.items():
            print(f"  Processing scenario: {scenario_name}")

            flow_key = next((k for k in scenario_data.keys() if "flow" in k.lower()), None)

            if not flow_key:
                print(f"    No flow key found in {scenario_name}")
                continue

            flowfile_paths = scenario_data[flow_key]
            if not flowfile_paths:
                print(f"    No flowfiles to combine for {scenario_name}")
                continue

            collection_output_dir = os.path.join(output_dir, collection_name)
            os.makedirs(collection_output_dir, exist_ok=True)

            output_filename = f"{collection_name}_{scenario_name}_combined_flowfile.csv"
            output_path = os.path.join(collection_output_dir, output_filename)

            try:
                combine_flowfiles(flowfile_paths, output_path)
                results[collection_name][scenario_name] = output_path
            except Exception as e:
                print(f"    Error combining flowfiles for {scenario_name}: {e}")
                continue

    return results


def main():
    """
    Combine flowfiles from STAC query results JSON.

    Reads STAC query results and combines all flowfiles for each scenario/event.
    Expected STAC query output structure: {collection: {scenario: {flow_key: [paths], ...}}}
    """

    parser = argparse.ArgumentParser(description="Combine flowfiles from STAC query results")
    parser.add_argument("stac_json", nargs="?", help="Path to STAC query results JSON file")
    parser.add_argument(
        "-o",
        "--output-dir",
        default="combined_flowfiles",
        help="Output directory for combined flowfiles (default: combined_flowfiles)",
    )
    parser.add_argument(
        "--test-output",
        action="store_true",
        help="Generate test data and run in test mode",
    )

    args = parser.parse_args()

    if args.test_output:
        print("Running in test mode...")
        run_test_mode()
        return

    else:
        if args.stac_json is None:
            parser.error("stac_json is required when not in test mode")

        results = process_stac_query_results(args.stac_json, args.output_dir)

    print("\n=== Summary ===")
    for collection, scenarios in results.items():
        print(f"{collection}:")
        for scenario, path in scenarios.items():
            print(f"  {scenario}: {path}")


if __name__ == "__main__":
    main()
