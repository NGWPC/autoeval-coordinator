#!/usr/bin/env python3

import argparse
import datetime
import json
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Set

import boto3
from botocore.exceptions import ClientError


def setup_arguments():
    """Sets up and parses command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Analyzes pipeline logs to categorize AOIs, providing detailed intermediate outputs for verification.",
        epilog="""Examples:
  # Use default 7 days back
  %(prog)s run_list.txt batch_name output_dir

  # Use specific date-hour range
  # For >10k results, use the AWS console to find message timestamps, then query smaller ranges.
  %(prog)s run_list.txt batch_name output_dir 2025-08-06-12 2025-08-09-18
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "run_list",
        type=Path,
        help="Path to file containing list of AOIs to process",
    )
    parser.add_argument(
        "batch_name",
        help="Batch name for filtering logs (e.g., fim100_huc12_10m_2025-08-06-12)",
    )
    parser.add_argument(
        "output_dir",
        type=Path,
        help="Directory where ALL output files will be written",
    )
    parser.add_argument(
        "start_datetime",
        nargs="?",
        help="Start date-time in YYYY-MM-DD-HH format (optional)",
    )
    parser.add_argument(
        "end_datetime",
        nargs="?",
        help="End date-time in YYYY-MM-DD-HH format (optional)",
    )
    return parser.parse_args()


def run_query(
    logs_client, log_group: str, start_time: int, end_time: int, query: str
) -> List[Dict]:
    """Runs a CloudWatch Logs Insights query and returns the results."""
    try:
        start_query_response = logs_client.start_query(
            logGroupName=log_group,
            startTime=start_time,
            endTime=end_time,
            queryString=query,
        )
        query_id = start_query_response["queryId"]
        status = "Running"
        while status in ["Running", "Scheduled"]:
            print(f"  Query status: {status}. Waiting...")
            time.sleep(5)
            results_response = logs_client.get_query_results(queryId=query_id)
            status = results_response["status"]

        if status != "Complete":
            print(
                f"Error: Query failed with status '{status}'", file=sys.stderr
            )
            return []

        results = results_response.get("results", [])
        if len(results) == 10000:
            print(
                "  WARNING: Query returned 10,000 results (CloudWatch limit). Results may be incomplete."
            )

        return results
    except ClientError as e:
        print(f"An AWS error occurred: {e}", file=sys.stderr)
        return []


def extract_aois_from_results(results: List[Dict]) -> Set[str]:
    """Extracts unique AOI names from structured query results."""
    aois = set()
    aoi_pattern = re.compile(r"aoi_name=([^,\]]+)")
    for row in results:
        for field in row:
            if field["field"] == "@logStream":
                match = aoi_pattern.search(field["value"])
                if match:
                    aois.add(match.group(1))
    return aois


def write_data_to_json(file_path: Path, data: List[Dict]):
    """Writes raw query results to a pretty-printed JSON file."""
    print(f"  Writing {len(data)} raw results to {file_path}")
    with file_path.open("w") as f:
        json.dump(data, f, indent=2)


def write_aois_to_file(file_path: Path, aois: Set[str]):
    """Writes a sorted set of AOIs to a text file."""
    print(f"  Writing {len(aois)} AOIs to {file_path}")
    with file_path.open("w") as f:
        for aoi in sorted(list(aois)):
            f.write(f"{aoi}\n")


def main():
    """Main function to orchestrate the pipeline analysis."""
    args = setup_arguments()

    if not args.run_list.is_file():
        print(
            f"Error: Run list file not found: {args.run_list}", file=sys.stderr
        )
        sys.exit(1)

    # Determine time range
    if args.start_datetime and args.end_datetime:
        try:
            start_dt = datetime.datetime.strptime(
                args.start_datetime, "%Y-%m-%d-%H"
            )
            end_dt = datetime.datetime.strptime(
                args.end_datetime, "%Y-%m-%d-%H"
            ).replace(minute=59, second=59)
            time_range_desc = f"{args.start_datetime} to {args.end_datetime}"
        except ValueError:
            print(
                "Error: Invalid datetime format. Use YYYY-MM-DD-HH.",
                file=sys.stderr,
            )
            sys.exit(1)
    else:
        end_dt = datetime.datetime.now(datetime.timezone.utc)
        start_dt = end_dt - datetime.timedelta(days=7)
        time_range_desc = "last 7 days"

    start_time_ts = int(start_dt.timestamp())
    end_time_ts = int(end_dt.timestamp())

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print(
        f"Starting analysis for batch: {args.batch_name}\nTime range: {time_range_desc}"
    )

    log_group = "/aws/ec2/nomad-client-linux-test"
    queries = {
        "success": """
            fields @logStream
            | filter @logStream like /pipeline\\/dispatch-\\[batch_name={0}/
            | filter strcontains(@message, "Pipeline SUCCESS")
            | dedup @logStream
        """.strip(),
        "early_exit": """
            fields @logStream
            | filter @logStream like /pipeline\\/dispatch-\\[batch_name={0}/
            | filter strcontains(@message, "Pipeline exiting early")
            | dedup @logStream
        """.strip(),
        "failed": """
            fields @logStream
            | filter @logStream like /pipeline\\/dispatch-\\[batch_name={0}/
            | filter strcontains(@message, "Pipeline FAILED")
            | dedup @logStream
        """.strip(),
        "job_errors": """
            fields @logStream, @message
            | filter @logStream like /pipeline\\/dispatch-\\[batch_name={0}/
            | filter @message like /OOM|HTTPConnectionPool|BaseNomadException|JobStatus\\.(LOST|STOPPED|CANCELLED)|botocore\\.exceptions\\.ClientError/
            | dedup @logStream
        """.strip(),
        "agr_mos_errors": """
            fields @logStream, @message
            | filter @logStream like /(agreement_maker|fim_mosaicker)\\/dispatch-\\[batch_name={0}/
            | filter (@message like /[Ee][Rr][Rr][Oo][Rr]/ or @message like /[Ww][Aa][Rr][Nn]/)
                and @message not like /Agreement map contains no valid data/
                and @message not like /distributed\\.shuffle/
                and @message not like /No features found/
                and @message not like /Worker is at/
                and @message not like /distributed\\.worker\\.memory - WARNING - Unmanaged memory use is high/
                and @message not like /gc\\.collect/
                and @message not like /Warning 1: Request for \\d+-\\d+ failed with response_code=\\d+/
                and @message not like /Failed to communicate with scheduler during heartbeat/
                and @message not like /NotGeoreferencedWarning/
                and @message not like /distributed\\.comm\\.core\\.CommClosedError/
                and @message not like /UserWarning: Sending large graph/
                and @message not like /warnings\\.warn/
                and @message not like /has GPKG application_id/
                and @message not like /distributed\\.nanny - WARNING - Worker process still alive/
            | dedup @logStream
        """.strip(),
        "inundate_errors": """
            fields @logStream, @message
            | filter @logStream like /hand_inundator\\/dispatch-\\[batch_name={0}/
            | filter (@message like /[Ee][Rr][Rr][Oo][Rr]/ or @message like /[Ww][Aa][Rr][Nn]/)
                and @message not like /No matching forecast data/
                and @message not like /'NoneType' is not iterable/
                and @message not like /No catchments with negative LakeID/
            | dedup @logStream
        """.strip(),
        "ignorable_errors": """
            fields @logStream, @message
            | filter @logStream like /(agreement_maker|fim_mosaicker)\\/dispatch-\\[batch_name={0}/
            | filter @message like /Agreement map contains no valid data/
            | dedup @logStream
        """.strip(),
    }

    # --- Execute all queries, write intermediate files, and gather AOIs ---
    logs_client = boto3.client("logs", region_name="us-east-1")
    aoi_sets = {}
    for name, query_template in queries.items():
        print(f"\nRunning query for '{name}' pipelines...")
        query = query_template.format(args.batch_name)
        results = run_query(
            logs_client, log_group, start_time_ts, end_time_ts, query
        )

        # Write intermediate files for this query
        write_data_to_json(args.output_dir / f"{name}_results.json", results)
        aois = extract_aois_from_results(results)
        # don't write ignorable errors here since need to write truly ignorable errored out aoi's after taking into account aoi's that have both ignorable errrors and valid errors
        # don't write success or failed aois since we write final results at the end
        if name not in ["ignorable_errors", "success", "failed"]:
            write_aois_to_file(args.output_dir / f"{name}_aois.txt", aois)

        aoi_sets[name] = aois

    # --- Categorize AOIs using set logic, writing key intermediate sets ---
    print("\nCategorizing all AOIs...")
    initial_aois = {
        line.strip() for line in args.run_list.open() if line.strip()
    }
    write_aois_to_file(args.output_dir / "sorted-run-list.txt", initial_aois)

    successful_aois = aoi_sets["success"]
    successful_aois.update(aoi_sets["early_exit"])

    failed_aois = aoi_sets["failed"]
    all_found_aois = successful_aois.union(failed_aois)
    silent_failures = initial_aois - all_found_aois
    # Write the silent failures list, a key intermediate output
    write_aois_to_file(args.output_dir / "silent_failures.txt", silent_failures)

    failed_aois.update(silent_failures)
    failed_aois.update(aoi_sets["job_errors"])
    failed_aois.update(aoi_sets["agr_mos_errors"])
    failed_aois.update(aoi_sets["inundate_errors"])

    successful_aois -= failed_aois

    # AOIs with ignorable errors but no real errors (agr_mos or inundate) should be considered successful
    real_error_aois = aoi_sets["agr_mos_errors"].union(
        aoi_sets["inundate_errors"]
    )
    truly_ignorable = aoi_sets["ignorable_errors"] - real_error_aois
    write_aois_to_file(
        args.output_dir / "ignorable_errors_aois.txt", truly_ignorable
    )
    successful_aois.update(truly_ignorable)
    failed_aois -= truly_ignorable

    # --- Final Summary and Output ---
    print("\n" + "=" * 10 + " SUMMARY " + "=" * 10)
    print(f"Time range queried: {time_range_desc}")
    print(f"Input AOIs:      {len(initial_aois)}")
    print(f"Successful AOIs: {len(successful_aois)}")
    print(f"Failed AOIs:     {len(failed_aois)}")
    total_output = len(successful_aois) + len(failed_aois)
    print(f"Total processed: {total_output}\n")

    # Write final output files
    write_aois_to_file(
        args.output_dir / "unique_success_aoi_names.txt", successful_aois
    )
    write_aois_to_file(
        args.output_dir / "unique_fail_aoi_names.txt", failed_aois
    )

    if len(initial_aois) != total_output:
        missing_aois = initial_aois - successful_aois - failed_aois
        print(
            f"WARNING: Input count ({len(initial_aois)}) does not match output count ({total_output})!"
        )
        print(f"Missing AOIs: {len(missing_aois)}")
        write_aois_to_file(args.output_dir / "missing_aois.txt", missing_aois)
    else:
        print("All AOIs accounted for!")

    print("\nAnalysis complete!")


if __name__ == "__main__":
    main()
