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

from extract_wbd_geometries import extract_geometry_by_huc, clear_wbd_client_cache


def submit_pipeline_job(
    nomad_client: nomad.Nomad,
    huc_code: str,
    gpkg_path: str,
    batch_name: str,
    output_root: str,
    hand_index_path: str,
    benchmark_sources: str,
    nomad_token: Optional[str] = None,
    use_local_creds: bool = False,
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
        "nomad_token": nomad_token or os.environ.get("NOMAD_TOKEN", ""),
        "registry_token": os.environ.get("REGISTRY_TOKEN", ""),
    }
    
    # Include AWS credentials from environment if using local creds
    if use_local_creds:
        meta.update({
            "aws_access_key": os.environ.get("AWS_ACCESS_KEY_ID", ""),
            "aws_secret_key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        })

    try:
        result = nomad_client.job.dispatch_job(
            id_="pipeline", payload=None, meta=meta, id_prefix_template=f"{batch_name}-{huc_code}"
        )

        return result["DispatchedJobID"]

    except Exception as e:
        logging.error(f"Failed to dispatch job for HUC {huc_code}: {e}")
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

        logging.debug(
            f"Found {running_count} active pipeline jobs out of {len(pipeline_jobs)} total pipeline jobs"
        )
        return running_count
    except Exception as e:
        logging.warning(f"Failed to get running job count: {e}")
        return 0


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

    # Clear WBD client cache to free resources after all extractions
    clear_wbd_client_cache()
    
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
        "--use-local-creds", action="store_true",
        help="Use AWS credentials from shell environment instead of IAM roles"
    )

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

    # Initialize fsspec S3 filesystem
    s3_fs = fsspec.filesystem("s3")

    # Upload AOI files to S3
    s3_aoi_paths = {}
    s3_base = f"{args.output_root.rstrip('/')}/{args.batch_name}/huc_aois"

    logging.info(f"Uploading AOI files to {s3_base}")
    for huc_code, local_path in huc_files.items():
        s3_path = f"{s3_base}/huc_{huc_code}.gpkg"
        try:
            with open(local_path, "rb") as local_file:
                with s3_fs.open(s3_path, "wb") as s3_file:
                    s3_file.write(local_file.read())
            s3_aoi_paths[huc_code] = s3_path
            logging.info(f"Uploaded {local_path} to {s3_path}")
        except Exception as e:
            logging.error(f"Failed to upload AOI for HUC {huc_code}: {e}")
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

    # Process HUCs - submit all jobs immediately
    submitted_jobs = []
    failed_submissions = []

    logging.info(f"Starting job submission for {len(s3_aoi_paths)} HUCs")

    for huc_code, s3_path in s3_aoi_paths.items():
        # Wait for pipeline slots if max limit specified
        if args.max_pipelines is not None:
            while True:
                current_jobs = get_running_pipeline_jobs(nomad_client)
                # Add 1 to account for the parameterized job template that's always running
                if current_jobs < args.max_pipelines + 1:
                    break
                wait_time = max(args.wait_seconds, 10)  # Minimum 10 seconds to avoid hammering the API
                logging.info(
                    f"Maximum pipeline limit ({args.max_pipelines}) reached. Current jobs: {current_jobs}. Waiting {wait_time} seconds..."
                )
                time.sleep(wait_time)

        logging.info(f"Submitting job for HUC {huc_code}")

        try:
            job_id = submit_pipeline_job(
                nomad_client=nomad_client,
                huc_code=huc_code,
                gpkg_path=s3_path,
                batch_name=args.batch_name,
                output_root=args.output_root,
                hand_index_path=args.hand_index_path,
                benchmark_sources=args.benchmark_sources,
                nomad_token=args.nomad_token,
                use_local_creds=args.use_local_creds,
            )

            submitted_jobs.append((huc_code, job_id))
            logging.info(f"Successfully submitted job {job_id} for HUC {huc_code}")

            # Wait between submissions if specified
            if args.wait_seconds > 0:
                logging.info(f"Waiting {args.wait_seconds} seconds before next submission...")
                time.sleep(args.wait_seconds)

        except Exception as e:
            logging.error(f"Failed to submit job for HUC {huc_code}: {e}")
            failed_submissions.append((huc_code, str(e)))

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

    # Monitor running jobs until all are complete
    if submitted_jobs:
        logging.info("\nMonitoring job completion...")
        while True:
            current_jobs = get_running_pipeline_jobs(nomad_client)
            logging.info(f"Currently running pipeline jobs: {current_jobs}")
            
            if current_jobs <= 1:  # Only the parameterized job template should remain
                logging.info("All submitted jobs have completed!")
                break
            
            # Wait before checking again
            time.sleep(60)  # Check every minute

    logging.info(f"\nCleaning up S3 AOI directory: {s3_base}")
    try:
        s3_fs.rm(s3_base, recursive=True)
        logging.info("Successfully cleaned up S3 AOI files")
    except Exception as e:
        logging.warning(f"Failed to cleanup S3 directory: {e}")

    return 0 if not failed_submissions else 1


if __name__ == "__main__":
    sys.exit(main())
