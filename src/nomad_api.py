import asyncio
import json
import logging
from urllib.parse import urlparse

import nomad  # python-nomad client
import aiohttp
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
        self.events = self.EventsWrapper(
            session=self._session,
            base_url=str(config.nomad.address).rstrip("/"),
            namespace=config.nomad.namespace,
            headers={
                "Content-Type": "application/json",
                **({"X-Nomad-Token": config.nomad.token} if config.nomad.token else {}),
            },
        )

    async def close(self):
        # python-nomad uses requests under the hood; nothing to close here
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
    async def dispatch_job(self, job_name: str, job_prefix: str, meta: dict) -> dict:
        """
        Dispatches a parameterized job via python-nomad in a threadpool.
        Returns the raw response dict which must contain 'DispatchedJobID' (and usually 'EvalID').
        """

        def _sync_dispatch():
            try:
                # python-nomad signature: job.dispatch(job, meta=None, job_id_prefix=None)
                return self._client.job.dispatch(
                    job=job_name,
                    meta=meta,
                    job_id_prefix=job_prefix,
                )
            except URLNotFoundNomadException as e:
                # 404 → job name not found
                raise LookupError(f"Nomad job '{job_name}' not found") from e
            except NomadAPIException as e:
                # generic 4xx/5xx from Nomad
                raise ConnectionError(
                    f"Nomad API error dispatching '{job_name}': {e}"
                ) from e

        logging.info("Dispatching Nomad job %r (prefix=%r)", job_name, job_prefix)
        resp = await asyncio.to_thread(_sync_dispatch)

        if not isinstance(resp, dict) or "DispatchedJobID" not in resp:
            raise RuntimeError(f"Bad dispatch response for {job_name!r}: {resp!r}")

        return resp

    class EventsWrapper:
        """
        Async generator over Nomad's /v1/event/stream SSE endpoint.
        """

        def __init__(
            self,
            session: aiohttp.ClientSession,
            base_url: str,
            namespace: str,
            headers: dict,
        ):
            self._session = session
            self._base_url = base_url
            self._namespace = namespace
            self._headers = headers
            self._stream_index = 0

        async def stream(self, topics: dict = None):
            """
            Yields individual event dicts from Nomad's event stream API.
            """
            if topics is None:
                # track all Job & Allocation events by default
                topics = {"Allocation": ["*"], "Job": ["*"]}

            url = f"{self._base_url}/v1/event/stream"
            params = {"index": self._stream_index}
            if self._namespace and self._namespace != "*":
                params["namespace"] = self._namespace

            # attach filters
            for topic, keys in topics.items():
                for key in keys:
                    params[f"topic.{topic}"] = key

            async with self._session.get(
                url, params=params, headers=self._headers, timeout=None
            ) as resp:
                resp.raise_for_status()
                async for line in resp.content:
                    chunk = line.strip()
                    if not chunk or chunk == b"{}":  # empty or heartbeat
                        continue
                    try:
                        batch = json.loads(chunk.decode("utf-8"))
                        # bump index so next call picks up where we left off
                        self._stream_index = batch.get("Index", self._stream_index)
                        for event in batch.get("Events", []):
                            yield event
                    except Exception as e:
                        logging.warning(
                            "Skipping invalid event chunk %r → %s", chunk[:200], e
                        )
