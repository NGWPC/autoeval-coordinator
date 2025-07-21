import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

import nomad

from extract_wbd_geometries import extract_geometry_by_huc


def get_pipeline_memory_usage(nomad_client: nomad.Nomad) -> Dict[str, Dict[str, int]]:
    """
    Get current cluster capacity information focusing on pipeline jobs only.

    Returns:
        Dict with 'total', 'pipeline_allocated', 'pipeline_queued', and 'pipeline_total' memory in MB
    """
    total_memory = 0
    pipeline_allocated_memory = 0
    pipeline_queued_memory = 0

    try:
        nodes = nomad_client.nodes.get_nodes()

        for node in nodes:
            if node.get("Status") == "ready":
                node_id = node["ID"]
                node_detail = nomad_client.node.get_node(node_id)

                node_resources = node_detail.get("NodeResources", {})
                if "Memory" in node_resources:
                    total_memory += node_resources["Memory"].get("MemoryMB", 0)

                allocations = nomad_client.node.get_allocations(node_id)

                for alloc in allocations:
                    if alloc.get("ClientStatus") == "running":
                        alloc_id = alloc["ID"]
                        alloc_detail = nomad_client.allocation.get_allocation(alloc_id)

                        # Check if this is a pipeline job by examining the job name
                        job_name = alloc_detail.get("JobID", "")
                        if job_name.startswith("pipeline/dispatch-"):
                            # Sum up memory from allocation resources for pipeline jobs only
                            alloc_resources = alloc_detail.get("AllocatedResources", {})
                            if "Shared" in alloc_resources:
                                pipeline_allocated_memory += alloc_resources["Shared"].get("MemoryMB", 0)

                            # Also check task resources
                            tasks = alloc_resources.get("Tasks", {})
                            for task_resources in tasks.values():
                                pipeline_allocated_memory += task_resources.get("Memory", {}).get("MemoryMB", 0)

        # Get all pipeline jobs to find queued ones
        jobs = nomad_client.jobs.get_jobs(prefix="pipeline/dispatch-")

        for job in jobs:
            job_id = job["ID"]
            job_detail = nomad_client.job.get_job(job_id)

            # Check if job is in pending state
            if job_detail.get("Status") == "pending":
                # Get memory requirements from job spec
                task_groups = job_detail.get("TaskGroups", [])
                for group in task_groups:
                    tasks = group.get("Tasks", [])
                    for task in tasks:
                        resources = task.get("Resources", {})
                        pipeline_queued_memory += resources.get("MemoryMB", 0)

        pipeline_total_memory = pipeline_allocated_memory + pipeline_queued_memory

        return {
            "total": {"memory": total_memory},
            "pipeline_allocated": {"memory": pipeline_allocated_memory},
            "pipeline_queued": {"memory": pipeline_queued_memory},
            "pipeline_total": {"memory": pipeline_total_memory},
        }

    except Exception as e:
        logging.error(f"Failed to get pipeline memory usage: {e}")
        raise


def submit_pipeline_job(
    nomad_client: nomad.Nomad,
    huc_code: str,
    gpkg_path: str,
    batch_name: str,
    output_root: str,
    hand_index_path: str,
    benchmark_sources: str,
) -> str:
    """
    Submit a pipeline job for a single HUC.

    Returns:
        Dispatched job ID
    """
    # Ensure output_root doesn't have redundant trailing slashes
    output_root_clean = output_root.rstrip("/")

    meta = {
        "aoi": str(gpkg_path),
        "outputs_path": f"{output_root_clean}/{huc_code}/",
        "hand_index_path": hand_index_path,
        "benchmark_sources": benchmark_sources,
        "tags": f"batch_name={batch_name} aoi_name={huc_code}",
        "aws_access_key": os.environ.get("AWS_ACCESS_KEY_ID", ""),
        "aws_secret_key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
    }

    try:
        result = nomad_client.job.dispatch_job(
            id_="pipeline", payload=None, meta=meta, id_prefix_template=f"{batch_name}-{huc_code}"
        )

        return result["DispatchedJobID"]

    except Exception as e:
        logging.error(f"Failed to dispatch job for HUC {huc_code}: {e}")
        raise


def extract_hucs(huc_codes: List[str], temp_dir: Path) -> Dict[str, Path]:
    """
    Extract HUC geometries and save as individual gpkg files.

    Returns:
        Dict mapping HUC codes to their gpkg file paths
    """
    huc_files = {}

    for huc_code in huc_codes:
        try:
            logging.info(f"Extracting geometry for HUC {huc_code}")

            # Extract geometry
            gdf = extract_geometry_by_huc(huc_code)

            # Save to temp file
            output_file = temp_dir / f"huc_{huc_code}.gpkg"
            gdf.to_file(output_file, driver="GPKG")

            huc_files[huc_code] = output_file
            logging.info(f"Saved HUC {huc_code} to {output_file}")

        except Exception as e:
            logging.error(f"Failed to extract HUC {huc_code}: {e}")
            # Continue with other HUCs

    return huc_files


def main():
    parser = argparse.ArgumentParser(description="Submit batch of pipeline jobs for multiple HUCs")

    # Required arguments
    parser.add_argument("--batch_name", required=True, help="Name for this batch of jobs (passed as tag)")
    parser.add_argument(
        "--output_root",
        required=True,
        help="Root directory for outputs (HUC code will be appended to create an individual pipelines output path)",
    )
    parser.add_argument("--hand_index_path", required=True, help="Path to HAND index (passed to pipeline)")
    parser.add_argument(
        "--benchmark_sources", required=True, help="Comma-separated benchmark sources (passed to pipeline)"
    )
    parser.add_argument("--huc_list", required=True, help="Path to text file with HUC codes (one per line)")

    # Optional arguments
    parser.add_argument(
        "--temp_dir", default="/tmp/huc_batch", help="Temporary directory for extracted HUC .gpkg files"
    )
    parser.add_argument(
        "--capacity_threshold_percent",
        type=float,
        default=15.0,
        help="Maximum percentage of total memory that pipeline jobs can use before blocking new submissions",
    )
    parser.add_argument("--wait_minutes", type=int, default=5, help="Minutes to wait between capacity checks")

    # Nomad connection arguments
    parser.add_argument(
        "--nomad_addr", default=os.environ.get("NOMAD_ADDR", "http://localhost:4646"), help="Nomad server address"
    )
    parser.add_argument(
        "--nomad_namespace", default=os.environ.get("NOMAD_NAMESPACE", "default"), help="Nomad namespace"
    )
    parser.add_argument("--nomad_token", default=os.environ.get("NOMAD_TOKEN"), help="Nomad ACL token")

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")

    # Create temp directory
    temp_dir = Path(args.temp_dir)
    temp_dir.mkdir(parents=True, exist_ok=True)

    # Read HUC codes
    with open(args.huc_list, "r") as f:
        huc_codes = [line.strip() for line in f if line.strip()]

    logging.info(f"Loaded {len(huc_codes)} HUC codes from {args.huc_list}")

    # Extract HUC geometries
    logging.info("Extracting HUC geometries...")
    huc_files = extract_hucs(huc_codes, temp_dir)

    if not huc_files:
        logging.error("No HUC geometries extracted successfully")
        return 1

    logging.info(f"Successfully extracted {len(huc_files)} HUC geometries")

    # Initialize Nomad client
    parsed = urlparse(args.nomad_addr)
    nomad_client = nomad.Nomad(
        host=parsed.hostname,
        port=parsed.port or 4646,
        verify=False,
        token=args.nomad_token,
        namespace=args.nomad_namespace,
    )

    # Process HUCs
    hucs_to_process = list(huc_files.keys())
    submitted_jobs = []
    failed_submissions = []

    logging.info(f"Starting job submission for {len(hucs_to_process)} HUCs")
    logging.info(f"Pipeline capacity threshold: {args.capacity_threshold_percent}%")
    logging.info(f"Wait time between checks: {args.wait_minutes} minutes")

    while hucs_to_process:
        try:
            # Check pipeline memory usage
            capacity = get_pipeline_memory_usage(nomad_client)
            total_memory = capacity["total"]["memory"]
            pipeline_allocated = capacity["pipeline_allocated"]["memory"]
            pipeline_queued = capacity["pipeline_queued"]["memory"]
            pipeline_total = capacity["pipeline_total"]["memory"]
            pipeline_usage_percent = (pipeline_total / total_memory * 100) if total_memory > 0 else 0

            logging.info(
                f"Pipeline memory usage - Total cluster: {total_memory} MB, "
                f"Pipeline allocated: {pipeline_allocated} MB, "
                f"Pipeline queued: {pipeline_queued} MB, "
                f"Pipeline total: {pipeline_total} MB ({pipeline_usage_percent:.1f}%)"
            )

            if pipeline_usage_percent < args.capacity_threshold_percent:
                # Submit a job
                huc_code = hucs_to_process.pop(0)
                gpkg_path = huc_files[huc_code]

                logging.info(f"Submitting job for HUC {huc_code} ({len(hucs_to_process)} remaining)")

                try:
                    job_id = submit_pipeline_job(
                        nomad_client=nomad_client,
                        huc_code=huc_code,
                        gpkg_path=gpkg_path,
                        batch_name=args.batch_name,
                        output_root=args.output_root,
                        hand_index_path=args.hand_index_path,
                        benchmark_sources=args.benchmark_sources,
                    )

                    submitted_jobs.append((huc_code, job_id))
                    logging.info(f"Successfully submitted job {job_id} for HUC {huc_code}")

                    # Wait to allow cluster autoscaling and prevent resource exhaustion
                    logging.info(f"Waiting {args.wait_minutes} minutes before next submission...")
                    time.sleep(args.wait_minutes * 60)

                except Exception as e:
                    logging.error(f"Failed to submit job for HUC {huc_code}: {e}")
                    failed_submissions.append((huc_code, str(e)))

            else:
                # Wait before checking again
                logging.info(
                    f"Pipeline capacity exceeded ({pipeline_usage_percent:.1f}% >= {args.capacity_threshold_percent}%). "
                    f"Waiting {args.wait_minutes} minutes before next check..."
                )
                time.sleep(args.wait_minutes * 60)

        except KeyboardInterrupt:
            logging.info("Interrupted by user")
            break

        except Exception as e:
            logging.error(f"Error during capacity check: {e}")
            logging.info(f"Waiting {args.wait_minutes} minutes before retry...")
            time.sleep(args.wait_minutes * 60)

    # Summary
    logging.info("\n" + "=" * 60)
    logging.info("BATCH SUBMISSION COMPLETE")
    logging.info("=" * 60)
    logging.info(f"Total HUCs processed: {len(huc_files)}")
    logging.info(f"Successfully submitted: {len(submitted_jobs)}")
    logging.info(f"Failed submissions: {len(failed_submissions)}")

    if submitted_jobs:
        logging.info("\nSubmitted jobs:")
        for huc_code, job_id in submitted_jobs:
            logging.info(f"  HUC {huc_code}: {job_id}")

    if failed_submissions:
        logging.info("\nFailed submissions:")
        for huc_code, error in failed_submissions:
            logging.info(f"  HUC {huc_code}: {error}")

    return 0 if not failed_submissions else 1


if __name__ == "__main__":
    sys.exit(main())
