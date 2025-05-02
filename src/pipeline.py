import asyncio
import logging
import tempfile
from typing import Any, Dict, List
import argparse
import json
import os
from uuid import uuid4
from pydantic import BaseModel

from data_service import DataService
from nomad_service import (
    InundationDispatchMeta,
    MosaicDispatchMeta,
    NomadService,
)
from load_config import AppConfig, load_config
import aiohttp
from nomad_api import NomadApiClient
from job_monitor import NomadJobMonitor


class PolygonPipeline:
    """
    Orchestrates one polygon pipeline:
      1) Query catchments
      2) For each catchment: write JSON → dispatch inundator → await
      3) Dispatch mosaicker → await
    """

    class Result(BaseModel):
        pipeline_id: str
        final_output: str

    def __init__(
        self,
        config: AppConfig,
        nomad: NomadService,
        data_svc: DataService,
        polygon: Dict[str, Any],
        pipeline_id: str,
    ):
        self.config = config
        self.nomad = nomad
        self.data_svc = data_svc
        self.polygon = polygon
        self.pipeline_id = pipeline_id
        # temp dir auto‐cleaned on .cleanup()
        self.tmp = tempfile.TemporaryDirectory(prefix=f"{pipeline_id}-")

        # Filled by initialize()
        self.catchments: Dict[str, Dict[str, Any]] = {}

    async def initialize(self) -> None:
        data = await self.data_svc.query_for_catchments(self.polygon)
        self.catchments = data.get("catchments", {})
        if not self.catchments:
            raise RuntimeError(f"[{self.pipeline_id}] no catchments found")
        logging.info(
            "[%s] init complete: %d catchments", self.pipeline_id, len(self.catchments)
        )

    async def run(self) -> Result:
        # 1) Initialize
        await self.initialize()

        # 2) Launch all inundator jobs concurrently
        inund_outputs: List[str] = []
        async with asyncio.TaskGroup() as tg:
            for catch_id, info in self.catchments.items():
                tg.create_task(self._process_catchment(catch_id, info, inund_outputs))

        # 3) Dispatch & await mosaicker
        mosaic_meta = MosaicDispatchMeta(
            pipeline_id=self.pipeline_id,
            raster_paths=inund_outputs,
            output_path=(
                f"s3://{self.config.s3.bucket}"
                f"/{self.config.s3.base_prefix}"
                f"/pipeline_{self.pipeline_id}/HAND_mosaic.tif"
            ),
            fim_type=self.config.defaults.fim_type,
            gdal_cache_max=str(self.config.defaults.gdal_cache_max),
            registry_token=self.config.nomad.registry_token or "",
            aws_access_key=self.config.s3.AWS_ACCESS_KEY_ID or "",
            aws_secret_key=self.config.s3.AWS_SECRET_ACCESS_KEY or "",
            aws_session_token=self.config.s3.AWS_SESSION_TOKEN or "",
        )
        mosaic_out = await self.nomad.run_job(
            self.config.jobs.fim_mosaicker,
            instance_prefix=f"mosaic-{self.pipeline_id[:8]}",
            meta=mosaic_meta.model_dump(),
        )

        return self.Result(pipeline_id=self.pipeline_id, final_output=mosaic_out)

    async def _process_catchment(
        self,
        catch_id: str,
        info: Dict[str, Any],
        collector: List[str],
    ) -> None:
        """
        1) write JSON to S3
        2) dispatch inundator
        3) await its output
        """
        base = (
            f"s3://{self.config.s3.bucket}"
            f"/{self.config.s3.base_prefix}"
            f"/pipeline_{self.pipeline_id}/catchment_{catch_id}"
        )
        meta = InundationDispatchMeta(
            pipeline_id=self.pipeline_id,
            catchment_data_path=f"{base}/catchment_data.json",
            forecast_path=self.config.mock_data_paths.forecast_csv,
            output_path=f"{base}/inundation_output.tif",
            fim_type=self.config.defaults.fim_type,
            gdal_cache_max=str(self.config.defaults.gdal_cache_max),
            registry_token=self.config.nomad.registry_token or "",
            aws_access_key=self.config.s3.AWS_ACCESS_KEY_ID or "",
            aws_secret_key=self.config.s3.AWS_SECRET_ACCESS_KEY or "",
            aws_session_token=self.config.s3.AWS_SESSION_TOKEN or "",
        )
        # a) write JSON
        await self.data_svc.write_json_to_uri(info, meta.catchment_data_path)
        logging.info(
            "[%s/%s] wrote catchment JSON → %s",
            self.pipeline_id,
            catch_id,
            meta.catchment_data_path,
        )

        # b) dispatch & await
        out = await self.nomad.run_job(
            self.config.jobs.hand_inundator,
            instance_prefix=f"inund-{self.pipeline_id[:8]}-{catch_id}",
            meta=meta.model_dump(),
        )
        collector.append(out)
        logging.info("[%s/%s] inundator done → %s", self.pipeline_id, catch_id, out)

    async def cleanup(self) -> None:
        """Remove tempdir."""
        self.tmp.cleanup()
        logging.info("[%s] cleaned up temp files", self.pipeline_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run one PolygonPipeline in isolation")
    parser.add_argument(
        "--polys",
        help="JSON file containing a list of polygon dicts",
    )
    parser.add_argument(
        "--index",
        type=int,
        default=0,
        help="Which polygon in the list to process",
    )
    parser.add_argument(
        "--config",
        default=os.path.join("config", "pipeline_config.yml"),
        help="Path to your YAML config",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    cfg = load_config(args.config)
    with open(args.polys, "r") as fp:
        polygons = json.load(fp)
    if not polygons:
        raise RuntimeError(f"No polygons found in {args.polygons_file!r}")
    polygon = polygons[args.index]

    async def _main():
        timeout = aiohttp.ClientTimeout(total=160, connect=40, sock_read=60)
        connector = aiohttp.TCPConnector(limit=cfg.defaults.http_connection_limit)
        async with aiohttp.ClientSession(
            timeout=timeout, connector=connector
        ) as session:

            api = NomadApiClient(cfg, session)
            monitor = NomadJobMonitor(api)
            await monitor.start()
            nomad = NomadService(api, monitor)

            data_svc = DataService(cfg)
            pid = "850"
            pipeline = PolygonPipeline(cfg, nomad, data_svc, polygon, pid)

            try:
                result = await pipeline.run()
                print(result.json(indent=2))
            finally:
                await pipeline.cleanup()
                await monitor.stop()

    asyncio.run(_main())
