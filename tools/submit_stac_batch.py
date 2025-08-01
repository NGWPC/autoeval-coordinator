import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

import fsspec
import nomad

from extract_stac_geometries import extract_geometry_by_stac_id, should_use_convex_hull


def submit_pipeline_job(
    nomad_client: nomad.Nomad,
    item_id: str,
    gpkg_path: str,
    batch_name: str,
    output_root: str,
    hand_index_path: str,
    benchmark_sources: str,
    nomad_token: Optional[str] = None,
    use_local_creds: bool = False,
) -> str:
    """
    Submit a pipeline job for a single STAC item.

    Returns:
        Dispatched job ID
    """
    # Ensure output_root doesn't have redundant trailing slashes
    output_root_clean = output_root.rstrip("/")

    meta = {
        "aoi": str(gpkg_path),
        "outputs_path": f"{output_root_clean}/{item_id}/",
        "hand_index_path": hand_index_path,
        "benchmark_sources": benchmark_sources,
        "tags": f"batch_name={batch_name} aoi_name={item_id}",
        "nomad_token": nomad_token or os.environ.get("NOMAD_TOKEN", ""),
        "registry_token": os.environ.get("REGISTRY_TOKEN", ""),
    }

    # Include AWS credentials from environment if using local creds
    if use_local_creds:
        meta.update(
            {
                "aws_access_key": os.environ.get("AWS_ACCESS_KEY_ID", ""),
                "aws_secret_key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
            }
        )

    # Create the id_prefix_template in the format [batch_name=value,aoi_name=value]
    id_prefix_template = f"[batch_name={batch_name},aoi_name={item_id}]"

    try:
        result = nomad_client.job.dispatch_job(
            id_="pipeline", payload=None, meta=meta, id_prefix_template=id_prefix_template
        )

        return result["DispatchedJobID"]

    except Exception as e:
        logging.error(f"Failed to dispatch job for STAC item {item_id}: {e}")
        raise


def get_running_pipeline_jobs(nomad_client: nomad.Nomad) -> int:
    """
    Get the count of running and queued pipeline jobs, including dispatched jobs waiting for allocation.

    Returns:
        Number of pipeline jobs in active states (not finished)
    """
    try:
        # Get all jobs to include dispatched jobs that haven't been allocated yet
        jobs = nomad_client.jobs.get_jobs()
        pipeline_jobs = [job for job in jobs if job.get("ID", "").startswith("pipeline")]

        running_count = 0
        for job in pipeline_jobs:
            job_status = job.get("Status", "")
            # Debug logging to see actual job statuses
            logging.debug(f"Pipeline job {job.get('ID', 'unknown')}: Status={job_status}")

            # Count jobs that are not finished (dead = finished)
            # "running" includes both allocated jobs and dispatched jobs waiting for allocation
            if job_status != "dead":
                running_count += 1

        logging.debug(f"Found {running_count} active pipeline jobs out of {len(pipeline_jobs)} total pipeline jobs")
        return running_count
    except Exception as e:
        logging.warning(f"Failed to get running job count: {e}")
        return 0


def extract_items(
    item_ids: List[str], temp_dir: Path, stac_api_url: str, collection: Optional[str] = None
) -> Dict[str, Path]:
    """
    Extract STAC item geometries and save as individual gpkg files.

    Returns:
        Dict mapping item IDs to their gpkg file paths
    """
    item_files = {}

    for item_id in item_ids:
        try:
            logging.info(f"Extracting geometry for STAC item {item_id}")

            # First, fetch without convex hull to get collection info
            gdf = extract_geometry_by_stac_id(
                item_id, stac_api_url=stac_api_url, collection=collection, use_convex_hull=False
            )

            # Check if we should use convex hull based on collection
            collection_id = gdf.iloc[0]["collection"]
            if should_use_convex_hull(collection_id):
                logging.info(f"Applying convex hull for collection {collection_id}")
                gdf = extract_geometry_by_stac_id(
                    item_id, stac_api_url=stac_api_url, collection=collection, use_convex_hull=True
                )

            # Save to temp file
            output_file = temp_dir / f"stac_{item_id}.gpkg"
            gdf.to_file(output_file, driver="GPKG")

            item_files[item_id] = output_file
            logging.info(f"Saved STAC item {item_id} to {output_file}")

        except Exception as e:
            logging.error(f"Failed to extract STAC item {item_id}: {e}")
            # Continue with other items

    return item_files


def main():
    parser = argparse.ArgumentParser(description="Submit batch of pipeline jobs for multiple STAC items")

    # Required arguments
    parser.add_argument("--batch_name", required=True, help="Name for this batch of jobs (passed as tag)")
    parser.add_argument(
        "--output_root",
        required=True,
        help="Root directory for outputs (STAC item ID will be appended to create an individual pipelines output path)",
    )
    parser.add_argument("--hand_index_path", required=True, help="Path to HAND index (passed to pipeline)")
    parser.add_argument(
        "--benchmark_sources", required=True, help="Comma-separated benchmark sources (passed to pipeline)"
    )
    parser.add_argument("--item_list", required=True, help="Path to text file with STAC item IDs (one per line)")

    # Optional arguments
    parser.add_argument(
        "--temp_dir", default="/tmp/stac_batch", help="Temporary directory for extracted STAC item .gpkg files"
    )
    parser.add_argument("--wait_seconds", type=int, default=0, help="Seconds to wait between job submissions")
    parser.add_argument(
        "--max_pipelines", type=int, help="Maximum number of pipeline jobs to allow running/queued concurrently"
    )

    # Nomad connection arguments
    parser.add_argument(
        "--nomad_addr", default=os.environ.get("NOMAD_ADDR", "http://localhost:4646"), help="Nomad server address"
    )
    parser.add_argument(
        "--nomad_namespace", default=os.environ.get("NOMAD_NAMESPACE", "default"), help="Nomad namespace"
    )
    parser.add_argument("--nomad_token", default=os.environ.get("NOMAD_TOKEN"), help="Nomad ACL token")

    # AWS authentication arguments
    parser.add_argument(
        "--use-local-creds", action="store_true", help="Use AWS credentials from shell environment instead of IAM roles"
    )

    # STAC-specific arguments
    parser.add_argument(
        "--stac_api_url", default="http://benchmark-stac.test.nextgenwaterprediction.com:8000", help="STAC API URL"
    )
    parser.add_argument("--collection", help="Optional: specific collection to search within")

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")

    # Create temp directory
    temp_dir = Path(args.temp_dir)
    temp_dir.mkdir(parents=True, exist_ok=True)

    # Read STAC item IDs
    with open(args.item_list, "r") as f:
        item_ids = [line.strip() for line in f if line.strip()]

    logging.info(f"Loaded {len(item_ids)} STAC item IDs from {args.item_list}")

    # Extract STAC item geometries
    logging.info("Extracting STAC item geometries...")
    item_files = extract_items(item_ids, temp_dir, args.stac_api_url, args.collection)

    if not item_files:
        logging.error("No STAC item geometries extracted successfully")
        return 1

    logging.info(f"Successfully extracted {len(item_files)} STAC item geometries")

    # Initialize fsspec S3 filesystem with default profile
    s3_fs = fsspec.filesystem("s3", profile="default")

    # Upload AOI files to S3
    s3_aoi_paths = {}
    s3_base = f"{args.output_root.rstrip('/')}/{args.batch_name}/stac_aois"

    logging.info(f"Uploading AOI files to {s3_base}")
    for item_id, local_path in item_files.items():
        s3_path = f"{s3_base}/stac_{item_id}.gpkg"
        try:
            with open(local_path, "rb") as local_file:
                with s3_fs.open(s3_path, "wb") as s3_file:
                    s3_file.write(local_file.read())
            s3_aoi_paths[item_id] = s3_path
            logging.info(f"Uploaded {local_path} to {s3_path}")
        except Exception as e:
            logging.error(f"Failed to upload AOI for STAC item {item_id}: {e}")
            continue

    if not s3_aoi_paths:
        logging.error("No AOI files uploaded successfully")
        return 1

    # Initialize Nomad client
    parsed = urlparse(args.nomad_addr)
    nomad_client = nomad.Nomad(
        host=parsed.hostname,
        port=parsed.port or 4646,
        verify=False,
        token=args.nomad_token,
        namespace=args.nomad_namespace,
    )

    # Process STAC items - submit all jobs immediately
    submitted_jobs = []
    failed_submissions = []

    logging.info(f"Starting job submission for {len(s3_aoi_paths)} STAC items")

    for item_id, s3_path in s3_aoi_paths.items():
        # Wait for pipeline slots if max limit specified
        if args.max_pipelines is not None:
            while True:
                current_jobs = get_running_pipeline_jobs(nomad_client)
                # Add 1 to account for the parameterized job template that's always running
                if current_jobs < args.max_pipelines + 1:
                    break
                wait_time = max(args.wait_seconds, 10)  # Minimum 10 seconds to avoid hammering the API
                logging.info(
                    f"Maximum pipeline limit ({args.max_pipelines}) reached. Current jobs: {current_jobs-1}. Waiting {wait_time} seconds..."  # current jobs is current chiled jobs so have to subtract 1 for parent
                )
                time.sleep(wait_time)

        logging.info(f"Submitting job for STAC item {item_id}")

        try:
            job_id = submit_pipeline_job(
                nomad_client=nomad_client,
                item_id=item_id,
                gpkg_path=s3_path,
                batch_name=args.batch_name,
                output_root=args.output_root,
                hand_index_path=args.hand_index_path,
                benchmark_sources=args.benchmark_sources,
                nomad_token=args.nomad_token,
                use_local_creds=args.use_local_creds,
            )

            submitted_jobs.append((item_id, job_id))
            logging.info(f"Successfully submitted job {job_id} for STAC item {item_id}")

            # Wait between submissions if specified
            if args.wait_seconds > 0:
                logging.info(f"Waiting {args.wait_seconds} seconds before next submission...")
                time.sleep(args.wait_seconds)

        except Exception as e:
            logging.error(f"Failed to submit job for STAC item {item_id}: {e}")
            failed_submissions.append((item_id, str(e)))

    # Summary
    logging.info("\n" + "=" * 60)
    logging.info("BATCH SUBMISSION COMPLETE")
    logging.info("=" * 60)
    logging.info(f"Total STAC items processed: {len(item_files)}")
    logging.info(f"Successfully submitted: {len(submitted_jobs)}")
    logging.info(f"Failed submissions: {len(failed_submissions)}")

    if submitted_jobs:
        logging.info("\nSubmitted jobs:")
        for item_id, job_id in submitted_jobs:
            logging.info(f"  STAC item {item_id}: {job_id}")

    if failed_submissions:
        logging.info("\nFailed submissions:")
        for item_id, error in failed_submissions:
            logging.info(f"  STAC item {item_id}: {error}")

    # Monitor running jobs until all are complete
    if submitted_jobs:
        logging.info("\nMonitoring job completion...")
        while True:
            current_jobs = get_running_pipeline_jobs(nomad_client)
            logging.info(f"Currently running pipeline jobs: {current_jobs-1}")  # don't count the parent job

            if current_jobs <= 1:  # Only the parameterized job template should remain
                logging.info("All submitted jobs have completed!")
                break

            # Wait before checking again
            time.sleep(60)  # Check every minute

    return 0 if not failed_submissions else 1


if __name__ == "__main__":
    sys.exit(main())
