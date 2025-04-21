import logging
import json
from typing import Dict, Optional, AsyncGenerator
from contextlib import asynccontextmanager, suppress
import asyncio
import aiohttp
from tenacity import (
    retry,
    stop_after_attempt,
    wait_fixed,
    retry_if_exception_type,
    RetryError,
)

from load_config import AppConfig

# Define which exceptions tenacity should retry on
NOMAD_RETRYABLE_EXCEPTIONS = (
    aiohttp.ClientConnectionError,  # Network issues
    aiohttp.ClientResponseError,  # Server-side errors (5xx)
    asyncio.TimeoutError,  # Request timeouts
    ConnectionError,  # Generic connection errors
)


# Custom retry logic for ClientResponseError (only retry 5xx)
def retry_if_nomad_server_error(exception):
    if isinstance(exception, aiohttp.ClientResponseError):
        return 500 <= exception.status < 600  # Retry only on 5xx errors
    return isinstance(exception, NOMAD_RETRYABLE_EXCEPTIONS)


class NomadApiClient:
    """Interacts with the Nomad HTTP API using aiohttp, with retries."""

    def __init__(
        self, config: AppConfig, session: aiohttp.ClientSession
    ):  # Accept session
        self.base_url = str(config.nomad.address).rstrip("/")
        self.namespace = config.nomad.namespace
        self._headers = {"Content-Type": "application/json"}
        if config.nomad.token:
            self._headers["X-Nomad-Token"] = config.nomad.token
        self._session = session  # Use the passed-in session
        self.events = self.EventsWrapper(
            self._session, self.base_url, self.namespace, self._headers
        )
        self.config = config

    async def close(self):
        # Session is managed externally, so this method does nothing now.
        logging.debug("NomadApiClient close called (session managed externally).")
        pass

    # Apply tenacity retry decorator
    @retry(
        stop=stop_after_attempt(3),  # Retry up to 3 times (1 initial + 2 retries)
        wait=wait_fixed(2),  # Wait 2 seconds between retries
        retry=retry_if_nomad_server_error,  # Custom retry condition
        reraise=True,  # Reraise the exception after retries exhausted
    )
    async def dispatch_job(
        self, job_name: str, job_instance_id: str, meta: Dict
    ) -> Dict:
        """Dispatches a pre-registered parameterized job with retries."""
        url = f"{self.base_url}/v1/job/{job_name}/dispatch"
        params = (
            {"namespace": self.namespace}
            if self.namespace and self.namespace != "*"
            else {}
        )
        payload = {"Meta": meta, "JobIDPrefix": job_instance_id}
        logging.info(f"Dispatching job '{job_name}' (prefix: {job_instance_id})")
        logging.debug(f"Dispatch Meta: {meta}")
        response: Optional[aiohttp.ClientResponse] = None
        try:
            # Use a reasonable timeout for the API call itself
            async with self._session.post(
                url, json=payload, params=params, headers=self._headers, timeout=30
            ) as response:
                # Check status before parsing JSON
                if response.status >= 400:
                    # Log specific error but let raise_for_status handle retry/reraise logic
                    logging.warning(
                        f"Nomad API response status {response.status} for dispatch '{job_name}'."
                    )
                    response.raise_for_status()  # Let tenacity handle retry based on status via retry_if_nomad_server_error

                response_data = await response.json()
                logging.debug(f"Nomad Dispatch Response Body: {response_data}")
                dispatched_id = response_data.get("DispatchedJobID")
                if not dispatched_id:
                    raise ValueError(
                        f"Dispatch response for {job_name} missing DispatchedJobID"
                    )
                logging.info(
                    f"Job '{job_name}' dispatched. EvalID: {response_data.get('EvalID')}, DispatchedJobID: {dispatched_id}"
                )
                return response_data
        except RetryError as e:
            # Logged when retries are exhausted
            logging.error(
                f"Dispatch failed for job '{job_name}' after multiple retries: {e.last_attempt.exception()}"
            )
            raise ConnectionError(
                f"Failed to dispatch job {job_name} after retries"
            ) from e.last_attempt.exception()
        except aiohttp.ClientResponseError as e:
            status = e.status
            message = e.message
            body = ""
            if response and response.content:
                with suppress(Exception):
                    body = await response.text()
            # Raise more specific standard errors for non-retryable client errors
            if status == 400:
                raise ValueError(
                    f"Nomad bad request dispatching {job_name}: {message}. Body: {body[:200]}"
                ) from e
            if status == 401 or status == 403:
                raise PermissionError(
                    f"Nomad permission error dispatching {job_name}: {message}"
                ) from e
            if status == 404:
                raise LookupError(f"Nomad job '{job_name}' not found: {message}") from e
            # If it wasn't retried (e.g., 4xx), raise a ConnectionError or let original propagate
            logging.error(
                f"Nomad API error dispatching job '{job_name}' ({status}): {message}. Body: {body[:500]}..."
            )
            raise ConnectionError(
                f"Nomad API error dispatching job {job_name} (status {status})"
            ) from e
        except aiohttp.ClientConnectionError as e:
            logging.error(
                f"Connection error dispatching job '{job_name}' to Nomad: {e}"
            )
            raise ConnectionError(
                f"Nomad connection error dispatching job {job_name}"
            ) from e
        except asyncio.TimeoutError as e:
            logging.error(f"Timeout dispatching job '{job_name}' to Nomad.")
            raise TimeoutError(
                f"Timeout dispatching job {job_name}"
            ) from e  # Standard TimeoutError
        except Exception as e:
            logging.exception(f"Unexpected error dispatching job '{job_name}': {e}")
            raise RuntimeError(f"Unexpected error dispatching job {job_name}") from e

    class EventsWrapper:
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

        @asynccontextmanager
        async def stream(self, topics: Dict = None) -> AsyncGenerator[Dict, None]:
            if topics is None:
                topics = {"Job": ["*"], "Allocation": ["*"]}
            url = f"{self._base_url}/v1/event/stream"
            params = {"index": self._stream_index}
            if self._namespace and self._namespace != "*":
                params["namespace"] = self._namespace
            for topic, keys in topics.items():
                for key in keys:
                    params[f"topic.{topic}"] = key
            logging.info(
                f"Connecting to Nomad event stream API: {url} with params: {params}"
            )
            response: Optional[aiohttp.ClientResponse] = None
            try:
                response = await self._session.get(
                    url, params=params, headers=self._headers, timeout=None
                )
                response.raise_for_status()
                logging.info("Nomad event stream connected via API.")

                async def event_generator():
                    try:
                        async for line in response.content:
                            if self._session.closed:
                                logging.warning(
                                    "Event stream stopping: Session closed."
                                )
                                break
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                event_list_wrapper = json.loads(line.decode("utf-8"))
                                if (
                                    isinstance(event_list_wrapper, list)
                                    and event_list_wrapper
                                ):
                                    wrapper = event_list_wrapper[0]
                                    if "Index" in wrapper and "Events" in wrapper:
                                        self._stream_index = wrapper["Index"]
                                        if wrapper["Events"]:
                                            for event in wrapper["Events"]:
                                                if event:
                                                    yield event
                                    else:
                                        yield event_list_wrapper
                                else:
                                    yield event_list_wrapper
                            except json.JSONDecodeError:
                                logging.warning(
                                    f"Event stream JSON decode error: {line[:100]}"
                                )
                            except Exception as e_gen:
                                logging.exception(
                                    f"Event stream processing error: {e_gen}"
                                )
                    # Let specific aiohttp errors propagate up
                    except aiohttp.ClientPayloadError as e_payload:
                        logging.error(f"Event stream payload error: {e_payload}")
                        raise
                    except aiohttp.ClientConnectionError as e_conn:
                        logging.error(f"Event stream connection error: {e_conn}")
                        raise
                    except asyncio.CancelledError:
                        logging.info("Event stream generator cancelled.")
                        raise
                    finally:
                        logging.info("Event stream generator finished.")

                yield event_generator()
            # Let setup errors propagate up
            except aiohttp.ClientResponseError as e:
                logging.error(
                    f"Failed to connect to event stream ({e.status}): {e.message}"
                )
                raise
            except aiohttp.ClientError as e:
                logging.error(f"Network error connecting to event stream: {e}")
                raise
            except asyncio.TimeoutError:
                logging.error("Timeout connecting to Nomad event stream API.")
                raise
            except Exception as e:
                logging.error(f"Unexpected error setting up event stream: {e}")
                raise
            finally:
                logging.info("Nomad event stream context exiting.")
                if response is not None:
                    response.release()
