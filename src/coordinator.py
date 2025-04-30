import asyncio
import logging
from uuid import uuid4
from typing import Any, Dict, List

from data_service import DataService
from job_monitor import NomadJobMonitor
from nomad_api import NomadApiClient
from nomad_service import NomadService
from pipeline import PolygonPipeline
from load_config import AppConfig


class PipelineCoordinator:
    """
    Spins up one PolygonPipeline per polygon, in parallel.
    """

    def __init__(self, config: AppConfig, session: Any):
        self.config = config
        self.api = NomadApiClient(config, session)
        self.monitor = NomadJobMonitor(self.api)
        self.nomad = NomadService(self.api, self.monitor)
        self.data_svc = DataService(config)

    async def run(self, polygons: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        await self.monitor.start()
        results: List[Dict[str, Any]] = []

        try:
            async with asyncio.TaskGroup() as tg:
                for poly in polygons:
                    pid = uuid4().hex
                    pipeline = PolygonPipeline(
                        self.config, self.nomad, self.data_svc, poly, pid
                    )
                    tg.create_task(self._run_one(pipeline, results))
        finally:
            await self.monitor.stop()
        return results

    async def _run_one(self, pipeline: PolygonPipeline, results: List[Dict]):
        try:
            res = await pipeline.run()
            results.append(res.model_dump())
        except Exception as exc:
            logging.error("[%s] failed: %s", pipeline.pipeline_id, exc, exc_info=True)
            results.append({"pipeline_id": pipeline.pipeline_id, "error": str(exc)})
        finally:
            await pipeline.cleanup()
