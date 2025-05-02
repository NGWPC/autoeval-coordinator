import asyncio
import json
import logging
import os
from typing import List, Dict, Any

import aiohttp

from load_config import load_config
from coordinator import PipelineCoordinator


async def main():
    # 1) load config & polygon data
    repo = os.path.dirname(os.path.dirname(__file__))
    cfg_path = os.path.join(repo, "config/pipeline_config.yml")
    cfg = load_config(cfg_path)

    with open(cfg.mock_data_paths.polygon_data_file) as f:
        polygons: List[Dict[str, Any]] = json.load(f)
    if not polygons:
        logging.error("No polygons to process.")
        return

    # 2) setup HTTP session
    timeout = aiohttp.ClientTimeout(total=160, connect=40, sock_read=60)
    connector = aiohttp.TCPConnector(limit=cfg.defaults.http_connection_limit)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as sess:
        coord = PipelineCoordinator(cfg, sess)
        results = await coord.run(polygons)

    # 3) print a summary
    succeeded = [r for r in results if "final_output" in r]
    failed = [r for r in results if "error" in r]
    logging.info("Pipelines succeeded: %d, failed: %d", len(succeeded), len(failed))
    for r in results:
        logging.info(" → %s", r)


if __name__ == "__main__":
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    asyncio.run(main())
