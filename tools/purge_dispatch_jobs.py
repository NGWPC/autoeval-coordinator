"""
Script to purge Nomad dispatch jobs using the API.
Replicates the functionality of:
for job in $(nomad job status agreement_maker | grep "dispatch-" | awk '{print $1}'); do
  echo "Purging $job";
  nomad job stop -purge $job;
done

But using the Nomad HTTP API for better performance.
By default purges dispatch jobs from all pipeline job definitions.
"""

import argparse
import asyncio
import logging
import os
import sys
from typing import List, Optional
from urllib.parse import urljoin

import aiohttp
import nomad
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# Add src to path to import config
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))
from default_config import (
    AGREEMENT_MAKER_JOB_NAME,
    FIM_MOSAICKER_JOB_NAME,
    HAND_INUNDATOR_JOB_NAME,
    NOMAD_ADDRESS,
    NOMAD_NAMESPACE,
    NOMAD_TOKEN,
)

# All pipeline job names
PIPELINE_JOBS = [
    AGREEMENT_MAKER_JOB_NAME,
    FIM_MOSAICKER_JOB_NAME,
    HAND_INUNDATOR_JOB_NAME,
    "pipeline",  # Add pipeline job
]

logger = logging.getLogger(__name__)


class NomadPurger:
    def __init__(self, nomad_addr: str, token: Optional[str] = None, namespace: str = "default"):
        self.nomad_addr = nomad_addr.rstrip("/")
        self.token = token
        self.namespace = namespace

        # Initialize the nomad client for some operations
        from urllib.parse import urlparse

        parsed = urlparse(nomad_addr)
        self.client = nomad.Nomad(
            host=parsed.hostname,
            port=parsed.port,
            verify=False,
            token=token or None,
            namespace=namespace or None,
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, nomad.api.exceptions.BaseNomadException)),
    )
    async def _api_call(self, session: aiohttp.ClientSession, method: str, path: str, **kwargs) -> dict:
        """Make a Nomad API call with retry logic."""
        url = urljoin(self.nomad_addr, path)
        headers = kwargs.pop("headers", {})

        if self.token:
            headers["X-Nomad-Token"] = self.token

        params = kwargs.pop("params", {})
        if self.namespace and self.namespace != "default":
            params["namespace"] = self.namespace

        async with session.request(method, url, headers=headers, params=params, **kwargs) as response:
            response.raise_for_status()
            return await response.json()

    async def get_all_dispatch_jobs(self, parent_job_names: List[str]) -> List[dict]:
        """Get all dispatch jobs for multiple parent jobs."""
        logger.info(f"Getting dispatch jobs for parent jobs: {', '.join(parent_job_names)}")

        async with aiohttp.ClientSession() as session:
            try:
                # Get all jobs at once
                all_jobs = await self._api_call(session, "GET", "/v1/jobs")

                dispatch_jobs = []

                for job in all_jobs:
                    job_id = job.get("ID", "")
                    # Check if this is a dispatch job (contains dispatch-)
                    if "dispatch-" in job_id:
                        # Check if it belongs to any of our parent jobs
                        for parent_name in parent_job_names:
                            if job_id.startswith(parent_name):
                                dispatch_jobs.append(job)
                                break

                logger.info(f"Found {len(dispatch_jobs)} dispatch jobs total")
                return dispatch_jobs

            except Exception as e:
                logger.error(f"Error getting dispatch jobs: {e}")
                raise

    async def purge_job(self, session: aiohttp.ClientSession, job_id: str, dry_run: bool = False) -> bool:
        """Purge a single job."""
        try:
            if dry_run:
                logger.info(f"[DRY RUN] Would purge job: {job_id}")
                return True

            logger.info(f"Purging job: {job_id}")

            # Stop and purge the job
            await self._api_call(session, "DELETE", f"/v1/job/{job_id}", params={"purge": "true"})

            logger.info(f"Successfully purged job: {job_id}")
            return True

        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                logger.warning(f"Job {job_id} not found (may have already been purged)")
                return True
            logger.error(f"Failed to purge job {job_id}: HTTP {e.status} - {e.message}")
            return False
        except Exception as e:
            logger.error(f"Failed to purge job {job_id}: {e}")
            return False

    async def purge_all_dispatch_jobs(self, parent_job_names: List[str], dry_run: bool = False) -> tuple[int, int]:
        """Purge all dispatch jobs for the given parent jobs with maximum concurrency."""
        dispatch_jobs = await self.get_all_dispatch_jobs(parent_job_names)

        if not dispatch_jobs:
            logger.info("No dispatch jobs found to purge")
            return 0, 0

        logger.info(f"Starting to purge {len(dispatch_jobs)} dispatch jobs with maximum concurrency")

        successful = 0
        failed = 0

        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=100),  # Increase connection pool
            timeout=aiohttp.ClientTimeout(total=30),
        ) as session:

            async def purge_single_job(job):
                job_id = job.get("ID", "unknown")
                success = await self.purge_job(session, job_id, dry_run)
                return job_id, success

            # Execute all purge operations concurrently without semaphore for max speed
            tasks = [purge_single_job(job) for job in dispatch_jobs]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Count results
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Task failed with exception: {result}")
                    failed += 1
                else:
                    job_id, success = result
                    if success:
                        successful += 1
                    else:
                        failed += 1

        logger.info(f"Purge complete: {successful} successful, {failed} failed")
        return successful, failed


async def main():
    """
    Script to purge Nomad dispatch jobs using the API.
    Replicates the functionality of:
    for job in $(nomad job status agreement_maker | grep "dispatch-" | awk '{print $1}'); do
      echo "Purging $job";
      nomad job stop -purge $job;
    done

    But using the Nomad HTTP API for better performance.
    By default purges dispatch jobs from all pipeline job definitions.
    """

    parser = argparse.ArgumentParser(
        description="Purge Nomad dispatch jobs using the API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Examples:
  %(prog)s                                    # Purge dispatch jobs from all pipeline jobs
  %(prog)s --job-name agreement_maker         # Purge only agreement_maker dispatch jobs
  %(prog)s --dry-run                          # Show what would be purged
  %(prog)s --nomad-addr http://nomad.example.com:4646

Default pipeline jobs: {', '.join(PIPELINE_JOBS)}
        """,
    )

    parser.add_argument(
        "--job-name",
        help="Specific parent job name to purge dispatch jobs for. If not specified, purges from all pipeline jobs.",
    )
    parser.add_argument(
        "--nomad-addr",
        default=os.getenv("NOMAD_ADDR", ""),
        help=f"Nomad server address",
    )
    parser.add_argument(
        "--nomad-token", default=os.getenv("NOMAD_TOKEN", NOMAD_TOKEN), help="Nomad authentication token"
    )
    parser.add_argument(
        "--namespace",
        default=os.getenv("NOMAD_NAMESPACE", "default"),
        help=f"Nomad namespace (default: {NOMAD_NAMESPACE})",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would be purged without actually doing it")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format="%(asctime)s - %(levelname)s - %(message)s")

    # Determine which jobs to target
    if args.job_name:
        target_jobs = [args.job_name]
        logger.info(f"Targeting specific job: {args.job_name}")
    else:
        target_jobs = PIPELINE_JOBS
        logger.info(f"Targeting all pipeline jobs: {', '.join(target_jobs)}")

    # Create purger and run
    purger = NomadPurger(
        nomad_addr=args.nomad_addr, token=args.nomad_token if args.nomad_token else None, namespace=args.namespace
    )

    try:
        successful, failed = await purger.purge_all_dispatch_jobs(target_jobs, dry_run=args.dry_run)

        if failed > 0:
            logger.error(f"Some operations failed: {successful} successful, {failed} failed")
            sys.exit(1)
        else:
            logger.info(f"All operations completed successfully: {successful} jobs processed")

    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
