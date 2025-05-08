#!/usr/bin/env python3
import argparse
import asyncio
import json
import logging
import os
import tempfile
from typing import Any, Dict, List
from uuid import uuid4

import aiohttp
from pydantic import BaseModel

from load_config import AppConfig, load_config
from data_service import DataService
from nomad_service import InundationDispatchMeta, MosaicDispatchMeta, NomadService
from nomad_api import NomadApiClient
from job_monitor import NomadJobMonitor


class PolygonPipeline:
    """
    1) Query catchments for a polygon
    2) For each catchment: write JSON → dispatch inundator → await results
    3) Mosaic all inundation outputs
    4) Query STAC API for flood‐extent rasters → mosaic them → await results
    """

    class Result(BaseModel):
        pipeline_id: str
        inundation_mosaic: str
        stac_extent_mosaic: str

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
        # for any temp files you might want to create
        self._tmpdir = tempfile.TemporaryDirectory(prefix=f"{pipeline_id}-")

        # Will be populated in initialize()
        self.catchments: Dict[str, Dict[str, Any]] = {}

    async def initialize(self) -> None:
        data = await self.data_svc.query_for_catchments(self.polygon)
        self.catchments = data.get("catchments", {})
        if not self.catchments:
            raise RuntimeError(f"pipeline [{self.pipeline_id}] no catchments found")
        logging.info(
            "[%s] init complete: %d catchments", self.pipeline_id, len(self.catchments)
        )

    async def run(self) -> Result:
        # load catchments
        await self.initialize()

        # launch all inundator jobs concurrently
        inundation_paths: List[str] = []
        async with asyncio.TaskGroup() as tg:
            for catch_id, info in self.catchments.items():
                tg.create_task(
                    self._process_catchment(catch_id, info, inundation_paths)
                )

        # mosaic inundation outputs
        mosaic1_meta = MosaicDispatchMeta(
            pipeline_id=self.pipeline_id,
            raster_paths=inundation_paths,
            output_path=(
                f"s3://{self.config.s3.bucket}/"
                f"{self.config.s3.base_prefix}/"
                f"pipeline_{self.pipeline_id}/HAND_mosaic.tif"
            ),
            fim_type=self.config.defaults.fim_type,
            gdal_cache_max=str(self.config.defaults.gdal_cache_max),
            registry_token=self.config.nomad.registry_token or "",
            aws_access_key=self.config.s3.AWS_ACCESS_KEY_ID or "",
            aws_secret_key=self.config.s3.AWS_SECRET_ACCESS_KEY or "",
            aws_session_token=self.config.s3.AWS_SESSION_TOKEN or "",
        )
        logging.info(
            "[%s] Dispatching HAND mosaicker for %d rasters",
            self.pipeline_id,
            len(inundation_paths),
        )
        inund_mosaic = await self.nomad.run_job(
            self.config.jobs.fim_mosaicker,
            instance_prefix=f"inund-mosaic-{self.pipeline_id[:8]}",
            meta=mosaic1_meta.model_dump(),
        )
        logging.info("[%s] HAND mosaic done → %s", self.pipeline_id, inund_mosaic)

        # query STAC & mosaic extents
        stac_groups = await self.data_svc.query_stac_groups(
            datetime=None,  # or pass a time window
            intersects=self.polygon,  # spatial filter
        )

        # flatten out just the "extents" TIFFs (skip hwm)
        extent_paths: List[str] = []
        for coll_short, groups in stac_groups.items():
            if coll_short.lower() == "hwm":
                continue
            for gid, assets in groups.items():
                for href in assets.get("extents", []):
                    extent_paths.append(href)
        logging.info(
            "[%s] Got %d STAC extents, dispatching mosaic",
            self.pipeline_id,
            len(extent_paths),
        )

        mosaic2_meta = MosaicDispatchMeta(
            pipeline_id=self.pipeline_id,
            raster_paths=extent_paths,
            output_path=(
                f"s3://{self.config.s3.bucket}/"
                f"{self.config.s3.base_prefix}/"
                f"pipeline_{self.pipeline_id}/STAC_extent_mosaic.tif"
            ),
            fim_type=self.config.defaults.fim_type,
            gdal_cache_max=str(self.config.defaults.gdal_cache_max),
            registry_token=self.config.nomad.registry_token or "",
            aws_access_key=self.config.s3.AWS_ACCESS_KEY_ID or "",
            aws_secret_key=self.config.s3.AWS_SECRET_ACCESS_KEY or "",
            aws_session_token=self.config.s3.AWS_SESSION_TOKEN or "",
        )
        ext_mosaic = await self.nomad.run_job(
            self.config.jobs.fim_mosaicker,
            instance_prefix=f"stac-mosaic-{self.pipeline_id[:8]}",
            meta=mosaic2_meta.model_dump(),
        )
        logging.info("[%s] STAC extent mosaic done → %s", self.pipeline_id, ext_mosaic)

        return self.Result(
            pipeline_id=self.pipeline_id,
            inundation_mosaic=inund_mosaic,
            stac_extent_mosaic=ext_mosaic,
        )

    async def _process_catchment(
        self,
        catch_id: str,
        info: Dict[str, Any],
        collector: List[str],
    ) -> None:
        """
        1) write catchment JSON to S3
        2) dispatch inundator job
        3) await output and append its path
        """
        base = (
            f"s3://{self.config.s3.bucket}/"
            f"{self.config.s3.base_prefix}/"
            f"pipeline_{self.pipeline_id}/catchment_{catch_id}"
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

        # write JSON
        await self.data_svc.write_json_to_uri(info, meta.catchment_data_path)
        logging.info(
            "[%s/%s] Wrote catchment JSON → %s",
            self.pipeline_id,
            catch_id,
            meta.catchment_data_path,
        )

        # run inundator job
        out = await self.nomad.run_job(
            self.config.jobs.hand_inundator,
            instance_prefix=f"inund-{self.pipeline_id[:8]}-{catch_id}",
            meta=meta.model_dump(),
        )
        collector.append(out)
        logging.info("[%s/%s] Inundator done → %s", self.pipeline_id, catch_id, out)

    async def cleanup(self) -> None:
        """Tear down any temporary files."""
        self._tmpdir.cleanup()
        logging.info("[%s] Cleaned up temp files", self.pipeline_id)


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
        help="Index into the polygon list to process",
    )
    parser.add_argument(
        "--config",
        default=os.path.join("config", "pipeline_config.yml"),
        help="Path to the YAML config",
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
        # HTTP session + Nomad client / monitor setup
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
                data = result.model_dump()
                print(json.dumps(data, indent=2))
            finally:
                await pipeline.cleanup()
                await monitor.stop()

    asyncio.run(_main())
