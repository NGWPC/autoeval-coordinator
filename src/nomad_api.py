import asyncio
import json
import logging
from typing import Dict, Any
from urllib.parse import urlparse

import aiohttp
import nomad  # python-nomad client
from tenacity import (
    retry,
    stop_after_attempt,
    wait_fixed,
    retry_if_exception_type,
)
from requests.exceptions import RequestException
from nomad.api.exceptions import (
    URLNotFoundNomadException,
    BaseNomadException,
    TimeoutNomadException,
)
from load_config import AppConfig


class EventsWrapper:
    """
    Streams Nomad's /v1/event/stream NDJSON endpoint,
    auto-reconnecting on network blips and resuming from last Index.
    """

    def __init__(
        self,
        session: aiohttp.ClientSession,
        base_url: str,
        namespace: str,
        headers: Dict[str, str],
    ):
        self._session = session
        self._url = base_url.rstrip("/") + "/v1/event/stream"
        self._headers = headers
        self._namespace = namespace or "*"
        self._index = 0

        # pre-build static params for topics + namespace
        self._base_params: Dict[str, Any] = {}
        if self._namespace != "*":
            self._base_params["namespace"] = self._namespace
        for topic in ("Job", "Allocation"):
            # mirror Nomad’s default topics of “*”
            self._base_params[f"topic.{topic}"] = "*"

    async def stream(self):
        backoff = 1
        while True:
            params = {"index": self._index, **self._base_params}

            try:
                async with self._session.get(
                    self._url,
                    headers=self._headers,
                    params=params,
                    timeout=None,  # no HTTP-level timeout
                ) as resp:
                    resp.raise_for_status()

                    # Even though this is `application/json`, we can still stream it:
                    content_type = resp.headers.get("content-type", "")
                    if "application/json" not in content_type:
                        logging.warning(
                            "Expected NDJSON, got Content-Type=%r", content_type
                        )

                    buffer = b""
                    async for chunk in resp.content.iter_chunked(8 * 1024):
                        buffer += chunk
                        while b"\n" in buffer:
                            line, buffer = buffer.split(b"\n", 1)
                            if not line or line == b"{}":
                                continue

                            batch = json.loads(line.decode("utf-8"))
                            # remember where we left off
                            self._index = batch.get("Index", self._index)

                            # yield each event in the batch
                            for ev in batch.get("Events", []):
                                yield ev

                    # if the server cleanly closed the stream, we'll loop and reconnect
                    logging.info("Event stream closed by server, reconnecting…")

            except asyncio.CancelledError:
                raise

            except Exception as e:
                logging.warning(
                    "NDJSON stream error %r – reconnecting in %ss", e, backoff
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)


class NomadApiClient:
    """
    Wraps python-nomad for dispatch and aiohttp-sse-client for streaming.
    """

    def __init__(self, config: AppConfig, session: aiohttp.ClientSession):
        self.config = config
        self._session = session

        # Parse the address into host/port/scheme
        parsed = urlparse(str(config.nomad.address))
        self._client = nomad.Nomad(
            host=parsed.hostname,
            port=parsed.port,
            verify=False,  # False to skip TLS verify
            token=config.nomad.token or None,
            namespace=config.nomad.namespace or None,
        )

        self.events = EventsWrapper(
            session=self._session,
            base_url=str(config.nomad.address),
            namespace=config.nomad.namespace,
            headers={
                "Content-Type": "application/json",
                **({"X-Nomad-Token": config.nomad.token} if config.nomad.token else {}),
            },
        )

    async def close(self):
        logging.debug("NomadApiClient.close() → no-op")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        retry=retry_if_exception_type(
            (
                aiohttp.ClientConnectionError,
                aiohttp.ClientResponseError,
                asyncio.TimeoutError,
                RequestException,  # covers things like ConnectionError, HTTPError
                TimeoutNomadException,  # raised on r.timeout inside python-nomad
                BaseNomadException,  # any 4xx/5xx or other API‐side errors
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
                return self._client.job.dispatch_job(
                    id_=job_name, payload=None, meta=meta, id_prefix_template=job_prefix
                )
            except URLNotFoundNomadException as e:
                # 404 → name not found
                raise LookupError(f"Nomad job '{job_name}' not found") from e
            except TimeoutNomadException as e:
                # upstream timeout → map to asyncio.TimeoutError so tenacity will retry if enabled
                raise asyncio.TimeoutError(f"Nomad dispatch timed out: {e}") from e
            except BaseNomadException as e:
                # catch all other API‐level faults
                raise RuntimeError(
                    f"Nomad API error dispatching '{job_name}': {e}"
                ) from e
            # any other RequestException (e.g. ConnectionError) will bubble out

        logging.info("Dispatching Nomad job %r (prefix=%r)", job_name, job_prefix)
        resp = await asyncio.to_thread(_sync_dispatch)

        if not isinstance(resp, dict) or "DispatchedJobID" not in resp:
            raise RuntimeError(f"Bad dispatch response for {job_name!r}: {resp!r}")

        return resp
