import asyncio
import json
import logging
import os
import uuid
from contextlib import suppress, AsyncExitStack
from typing import List, Dict, Any, Optional

import aiohttp
from pydantic import ValidationError

from load_config import load_config, AppConfig
from coordinator import PipelineCoordinator
from pipeline import PipelineState

# Define default timeouts for aiohttp session
DEFAULT_AIOHTTP_TIMEOUT = aiohttp.ClientTimeout(
    total=160,  # Total request timeout in seconds
    connect=40,  # Connection establishment timeout
    sock_read=60,  # Timeout for reading data from socket
)


async def run_main(config: AppConfig, multipolygon_data: List[Dict[str, Any]]):
    """Sets up resources, runs the coordinator, and ensures cleanup."""
    temp_dir = os.path.join(os.getcwd(), "temp_pipeline_data")
    os.makedirs(temp_dir, exist_ok=True)
    logging.info(f"Temporary data directory ensured: {temp_dir}")

    coordinator: Optional[PipelineCoordinator] = None
    all_pipeline_results = []

    async with AsyncExitStack() as stack:
        # --- Setup aiohttp session with defaults ---
        connector = aiohttp.TCPConnector(limit=config.defaults.http_connection_limit)
        http_session = await stack.enter_async_context(
            aiohttp.ClientSession(timeout=DEFAULT_AIOHTTP_TIMEOUT, connector=connector)
        )
        logging.info(
            f"aiohttp session created (timeout={DEFAULT_AIOHTTP_TIMEOUT.total}s, "
            f"conn_limit={config.defaults.http_connection_limit})."
        )

        # --- Initialize Coordinator with clients/sessions ---
        coordinator = PipelineCoordinator(config, http_session)

        # Use the passed data
        polygons_to_process = multipolygon_data

        try:
            if not polygons_to_process:
                logging.warning(
                    "No polygons provided to process. Exiting run_main early."
                )
                return

            logging.info(
                f"Running pipelines for {len(polygons_to_process)} polygon(s)..."
            )
            all_pipeline_results = await coordinator.run_multipolygon_pipeline(
                polygons_to_process
            )

            logging.info("\n--- Pipeline Run Summary ---")
            # Initialize counters using PipelineState enum members
            # Add specific counters for outcomes not directly in PipelineState
            status_counts = {state: 0 for state in PipelineState}
            status_counts["CANCELLED"] = 0  # Explicitly track cancellations
            status_counts["INVALID_STATUS"] = 0  # Track cases with bad status values

            for result in all_pipeline_results:
                pipeline_id = result.get("pipeline_id", "N/A")
                polygon_id = result.get("polygon_id", "N/A")
                status = result.get("status")  # Get status (might be None or invalid)
                error = result.get("error")
                result_data = result.get("result")

                log_status_str = "UNKNOWN"  # Default log string
                error_info = ""

                if isinstance(error, asyncio.CancelledError):
                    error_info = " Error: Task Cancelled"
                    status_counts["CANCELLED"] += 1
                    log_status_str = "CANCELLED"

                elif isinstance(error, BaseException):
                    error_info = f" Error: {type(error).__name__}: {error}"
                    status_counts[PipelineState.FAILED] += 1
                    log_status_str = PipelineState.FAILED.value
                    # Log a warning if the reported status wasn't FAILED
                    if (
                        isinstance(status, PipelineState)
                        and status != PipelineState.FAILED
                    ):
                        logging.warning(
                            f"Pipeline {pipeline_id} had error '{error_info}' but reported status was {status.value}. Final outcome counted as FAILED."
                        )
                    # Ensure status reflects failure if error occurred
                    status = PipelineState.FAILED

                elif isinstance(status, PipelineState):
                    # No error, and status is a valid PipelineState enum member
                    status_counts[status] += 1
                    log_status_str = status.value

                else:
                    # No error reported, but the status is not a valid PipelineState
                    logging.error(
                        f"Pipeline {pipeline_id} finished with unexpected status type: {type(status)} ({status}) and no reported error."
                    )
                    status_counts["INVALID_STATUS"] += 1
                    log_status_str = f"INVALID_STATUS ({status})"

                # Log the individual pipeline outcome
                logging.info(
                    f"Pipeline {pipeline_id} (Poly: {polygon_id}): Final outcome/state: {log_status_str}. "
                    f"Result: {result_data}.{error_info}"
                )

            # --- Log summary from counts ---
            summary_parts = []
            # Iterate through the collected counts
            for state, count in status_counts.items():
                if count > 0:
                    # Get the string representation for the summary key
                    state_name = (
                        state.value if isinstance(state, PipelineState) else str(state)
                    )
                    summary_parts.append(f"{count} {state_name}")

            # Check if specific states like FAILED, CANCELLED etc. had zero count but should be mentioned if non-zero
            # (The loop above already handles this correctly by checking count > 0)

            logging.info(
                f"\nSummary: {', '.join(summary_parts) or 'No pipelines run'}."
            )

        except KeyboardInterrupt:
            logging.warning("\nCtrl+C detected. Initiating graceful shutdown...")
            # Coordinator shutdown will be handled in `finally`
        except Exception:
            # Log critical errors during the overall pipeline execution phase
            logging.exception(
                "\nA critical error occurred during coordinator execution"
            )
        finally:
            logging.info("\nInitiating coordinator shutdown...")
            if coordinator:
                await coordinator.shutdown()  # Ensure coordinator's cleanup logic runs
            logging.info("Coordinator shutdown complete.")

    logging.info("Main function finished execution.")


if __name__ == "__main__":
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # --- Set log levels ---
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)

    # --- Load Config and Required Data Files ---
    config: Optional[AppConfig] = None
    multipolygon_data: List[Dict[str, Any]] = []
    config_file_path = os.environ.get("APP_CONFIG_PATH", "config.yaml")

    try:
        # --- Load Configuration ---
        logging.info(f"Loading configuration from: {config_file_path}")
        config = load_config(config_file_path)
        logging.info("Configuration loaded successfully.")

        # --- Check Mock Catchment Data File (Path from Config) ---
        mock_catchment_file = config.mock_data_paths.mock_catchment_data
        logging.info(f"Checking required file existence: {mock_catchment_file}")
        if not os.path.exists(mock_catchment_file):
            raise FileNotFoundError(
                f"Required mock catchment data file not found at path specified in config: '{mock_catchment_file}'"
            )
        logging.info(f"Required file found: {mock_catchment_file}")

        # --- Check and Load Polygon Data (Path from Config) ---
        polygon_data_path = config.mock_data_paths.polygon_data_file
        logging.info(f"Checking required file existence: {polygon_data_path}")
        if not os.path.exists(polygon_data_path):
            raise FileNotFoundError(
                f"Required polygon data file not found at path specified in config: '{polygon_data_path}'"
            )

        logging.info(f"Loading polygon data from: {polygon_data_path}")
        with open(polygon_data_path, "r") as f:
            try:
                loaded_data = json.load(f)
            except json.JSONDecodeError as json_err:
                raise ValueError(
                    f"Error decoding JSON from '{polygon_data_path}': {json_err}"
                ) from json_err

        if not isinstance(loaded_data, list):
            raise TypeError(
                f"Expected a list of polygons in '{polygon_data_path}', but got {type(loaded_data).__name__}"
            )

        multipolygon_data = loaded_data

        if not multipolygon_data:
            error_message = f"Polygon data file '{polygon_data_path}' was loaded but contained no polygons. Application cannot proceed."
            logging.error(error_message)
            raise ValueError(error_message)

        logging.info(
            f"Loaded {len(multipolygon_data)} polygon(s) from '{polygon_data_path}'."
        )

    except FileNotFoundError as fnf_err:
        logging.critical(f"Setup failed: Required file not found. {fnf_err}")
        exit(1)
    except (
        ValueError,
        TypeError,
        ValidationError,
    ) as config_data_err:
        logging.critical(
            f"Setup failed: Invalid configuration or data format. {config_data_err}"
        )
        exit(1)
    except Exception as setup_err:
        logging.critical(f"Fatal setup error: {setup_err}", exc_info=True)
        exit(1)

    # --- Run Main Application ---
    logging.info(
        "Configuration and data loaded successfully. Starting main application."
    )
    try:
        # Pass the validated config and loaded data
        asyncio.run(run_main(config, multipolygon_data))
    except Exception as main_err:
        # Catch any unhandled exceptions from asyncio.run() or run_main itself
        logging.critical(
            f"Unhandled exception during main execution: {main_err}", exc_info=True
        )
        exit(1)

    logging.info("Application finished.")
