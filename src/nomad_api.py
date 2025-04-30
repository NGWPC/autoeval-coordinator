import asyncio
import json
import logging
from urllib.parse import urlparse
from typing import Dict, Any

import aiohttp
import nomad  # python-nomad client
from tenacity import (
    retry,
    stop_after_attempt,
    wait_fixed,
    retry_if_exception_type,
)
from nomad.api.exceptions import (
    URLNotFoundNomadException,
    APIException as NomadAPIException,
)
from load_config import AppConfig


class EventsWrapper:
    """
    Async generator over Nomad's /v1/event/stream SSE endpoint,
    auto-reconnecting on network blips.
    """

    def __init__(
        self,
        session: aiohttp.ClientSession,
        base_url: str,
        namespace: str,
        headers: Dict[str, str],
    ):
        self._session = session
        self._base_url = base_url.rstrip("/")
        self._namespace = namespace
        self._headers = headers
        self._index = 0

    async def stream(self, topics: Dict[str, Any] = None):
        """
        Yields individual event dicts, reconnecting on errors
        and resuming from last index.
        """
        if topics is None:
            topics = {"Allocation": ["*"], "Job": ["*"]}

        backoff = 1
        while True:
            params = {"index": self._index}
            if self._namespace and self._namespace != "*":
                params["namespace"] = self._namespace

            # attach topic filters
            for topic, keys in topics.items():
                for key in keys:
                    params[f"topic.{topic}"] = key

            try:
                async with self._session.get(
                    f"{self._base_url}/v1/event/stream",
                    params=params,
                    headers=self._headers,
                    timeout=None,
                ) as resp:
                    resp.raise_for_status()
                    backoff = 1
                    async for line in resp.content:
                        chunk = line.strip()
                        if not chunk or chunk == b"{}":
                            continue
                        batch = json.loads(chunk.decode("utf-8"))
                        self._index = batch.get("Index", self._index)
                        for event in batch.get("Events", []):
                            yield event

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logging.warning(
                    "SSE stream dropped: %s – reconnecting in %ss", e, backoff
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

            except asyncio.CancelledError:
                raise

            except Exception:
                logging.exception("Unexpected error in SSE loop, reconnecting")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)


class NomadApiClient:
    """
    Wraps python-nomad for dispatch + aiohttp for SSE event streaming.
    """

    def __init__(self, config: AppConfig, session: aiohttp.ClientSession):
        self.config = config
        self._session = session

        # Parse the address into host/port/scheme
        parsed = urlparse(str(config.nomad.address))
        host = parsed.hostname
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        scheme = parsed.scheme

        # Instantiate python-nomad
        self._client = nomad.Nomad(
            host=host,
            port=port,
            scheme=scheme,
            token=(config.nomad.token or None),
            namespace=(config.nomad.namespace or None),
        )

        # SSE events wrapper
        self.events = EventsWrapper(
            session=self._session,
            base_url=str(config.nomad.address),
            namespace=config.nomad.namespace or "*",
            headers={
                "Content-Type": "application/json",
                **({"X-Nomad-Token": config.nomad.token} if config.nomad.token else {}),
            },
        )

    async def close(self):
        logging.debug("NomadApiClient.close() → no‐op")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        retry=retry_if_exception_type(
            (
                aiohttp.ClientConnectionError,
                aiohttp.ClientResponseError,
                asyncio.TimeoutError,
                ConnectionError,
            )
        ),
        reraise=True,
    )
    async def dispatch_job(
        self, job_name: str, job_prefix: str, meta: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Dispatches a parameterized job via python-nomad in a threadpool.
        Returns the raw response dict which must contain 'DispatchedJobID'.
        """

        def _sync_dispatch():
            try:
                return self._client.job.dispatch(
                    job=job_name,
                    meta=meta,
                    job_id_prefix=job_prefix,
                )
            except URLNotFoundNomadException as e:
                raise LookupError(f"Nomad job '{job_name}' not found") from e
            except NomadAPIException as e:
                raise ConnectionError(
                    f"Nomad API error dispatching '{job_name}': {e}"
                ) from e

        logging.info("Dispatching Nomad job %r (prefix=%r)", job_name, job_prefix)
        resp = await asyncio.to_thread(_sync_dispatch)

        if not isinstance(resp, dict) or "DispatchedJobID" not in resp:
            raise RuntimeError(f"Bad dispatch response for {job_name!r}: {resp!r}")

        return resp
