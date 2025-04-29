# src/nomad_service.py

import logging
from typing import Any, Dict

from pydantic import BaseModel

from nomad_api import NomadApiClient
from job_monitor import NomadJobMonitor


class DispatchMeta(BaseModel):
    pipeline_id: str
    catchment_data_path: str
    forecast_path: str
    output_path: str
    fim_type: str
    geo_mem_cache: str
    registry_token: str
    aws_access_key: str
    aws_secret_key: str
    aws_session_token: str


class NomadService:
    """
    Facade that combines:
      - NomadApiClient.dispatch_job()
      - NomadJobMonitor.track_job()
      - awaiting the Future to completion
    """

    def __init__(self, api: NomadApiClient, monitor: NomadJobMonitor):
        self.api = api
        self.monitor = monitor

    async def run_job(
        self, job_name: str, instance_prefix: str, meta: DispatchMeta
    ) -> str:
        """
        Dispatches a parameterized Nomad job, tracks it, and awaits its completion.

        Args:
          job_name: the Nomad job name (from config.jobs.<...>)
          instance_prefix: a short unique prefix for this invocation
          meta:       a Pydantic model containing all dispatch Meta keys

        Returns:
          The S3 output_path string from the job on success.

        Raises:
          RuntimeError or subclasses on dispatch‐ or execution‐failure.
        """
        # 1) Dispatch via python-nomad
        logging.info(
            "Dispatching Nomad job %r with prefix %r", job_name, instance_prefix
        )
        response: Dict[str, Any] = await self.api.dispatch_job(
            job_name, instance_prefix, meta.model_dump()
        )

        # python-nomad returns both EvalID and DispatchedJobID
        job_id = response.get("DispatchedJobID")
        if not job_id:
            raise RuntimeError(
                f"Dispatch response missing DispatchedJobID: {response!r}"
            )

        # 2) Register with the monitor
        future = await self.monitor.track_job(job_id, meta.model_dump())

        # 3) Await job completion (monitor resolves or fails the Future)
        output = await future

        # 4) Sanity‐check and return
        if not (isinstance(output, str) and output.startswith("s3://")):
            raise RuntimeError(f"Unexpected job output for {job_id!r}: {output!r}")

        logging.info("Nomad job %r completed successfully: %s", job_id, output)
        return output
