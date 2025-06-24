import asyncio
import json
import logging
from typing import Any, Dict, List

from pydantic import BaseModel, field_serializer

from job_monitor import NomadJobMonitor
from nomad_api import NomadApiClient


class DispatchMetaBase(BaseModel):
    """
    Common parameters for all dispatched jobs.
    """

    pipeline_id: str
    fim_type: str
    gdal_cache_max: str
    registry_token: str
    aws_access_key: str
    aws_secret_key: str
    aws_session_token: str


class InundationDispatchMeta(DispatchMetaBase):
    """
    Metadata for the HAND inundator job.
    """

    catchment_data_path: str
    forecast_path: str
    output_path: str


class MosaicDispatchMeta(DispatchMetaBase):
    # this is the new field
    raster_paths: List[str]
    output_path: str

    @field_serializer("raster_paths", mode="plain")
    def _ser_raster(self, v: List[str], info):
        # mosaic.py expects space-separated paths
        return " ".join(v)


class NomadService:
    ALLOC_TIMEOUT = 3600

    def __init__(self, api: NomadApiClient, monitor: NomadJobMonitor):
        self.api = api
        self.monitor = monitor

    async def run_job(
        self,
        job_name: str,
        instance_prefix: str,
        meta: Dict[str, Any],
    ) -> str:
        # 1) dispatch
        logging.info("Dispatching job %r (prefix=%r)", job_name, instance_prefix)
        resp = await self.api.dispatch_job(job_name, instance_prefix, meta)
        job_id = resp.get("DispatchedJobID")
        if not job_id:
            raise RuntimeError(f"Missing DispatchedJobID in {resp!r}")

        # 2) track & wait for allocation
        ctx = await self.monitor.track_job(job_id, meta)
        try:
            await asyncio.wait_for(ctx.alloc_fut, timeout=None)
            logging.info("Job %s allocated – now waiting for completion", job_id)
        except asyncio.TimeoutError:
            raise RuntimeError(f"Job {job_id} not allocated in {self.ALLOC_TIMEOUT}s")

        # 3) await terminal state (no timeout)
        output = await ctx.done_fut
        if not (isinstance(output, str) and output.startswith("s3://")):
            raise RuntimeError(f"Unexpected output for {job_id}: {output!r}")

        logging.info("Nomad job %s finished → %s", job_id, output)
        return output
