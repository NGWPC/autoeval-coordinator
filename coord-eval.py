#!/usr/bin/env python3
import json
import requests
import argparse
import os
import time
import boto3
from typing import Dict, Any, Set
from urllib.parse import urlparse
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel


class NomadCoordinator:
    def __init__(self, nomad_addr: str = "http://localhost:4646"):
        self.nomad_addr = nomad_addr
        self.headers = {"Content-Type": "application/json"}
        session = boto3.Session(profile_name="default")
        self.s3_client = session.client("s3")
        self.console = Console()

    def upload_catchment_data(
        self, catchment_data: dict, output_base: str, catchment_id: str
    ) -> str:
        """Upload catchment data to S3 and return the URL."""
        parsed = urlparse(output_base)
        bucket = parsed.netloc
        key = os.path.join(
            parsed.path.lstrip("/"), "temp", f"catchment_{catchment_id}.json"
        )
        self.s3_client.put_object(
            Bucket=bucket, Key=key, Body=json.dumps(catchment_data).encode("utf-8")
        )
        return f"s3://{bucket}/{key}"

    def submit_job(self, job_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Submit a parameterized job to Nomad."""
        url = f"{self.nomad_addr}/v1/job/inundation-processor/dispatch"
        dispatch_payload = {"Meta": job_metadata}
        self.console.print(
            f"Submitting job with metadata:\n{json.dumps(job_metadata, indent=2)}"
        )
        response = requests.post(url, json=dispatch_payload, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def monitor_job(self, job_prefix: str, max_retries: int = 5, retry_delay: int = 5):
        """Monitor job events using Nomad's event stream with automatic reconnection.

        Args:
            job_prefix: The job prefix to filter events for
            max_retries: Maximum number of reconnection attempts (-1 for infinite)
            retry_delay: Seconds to wait between reconnection attempts
        """
        uri = f"{self.nomad_addr}/v1/event/stream"

        # Add topic filters for job-related events
        params = {
            "topic": [
                f"Job:{job_prefix}*",  # Job events for our job
                "Evaluation",  # Evaluation events
                "Allocation",  # Allocation events
                "Deployment",  # Deployment events
            ]
        }

        retry_count = 0
        last_index = 0  # Track the last event index we've seen

        while True:
            try:
                self.console.print(
                    Panel(f"[yellow]Connecting to Nomad event stream at {uri}")
                )

                # Add index parameter to resume from last seen event
                if last_index > 0:
                    params["index"] = last_index

                with requests.get(
                    uri, params=params, stream=True, timeout=90
                ) as response:
                    response.raise_for_status()
                    self.console.print("[green]Connected to event stream")
                    retry_count = 0  # Reset retry count on successful connection

                    # Process the ndjson stream line by line
                    for line in response.iter_lines():
                        if line:
                            try:
                                message = json.loads(line)
                                # The Index could be at the message level or within Events
                                if "Index" in message:
                                    last_index = message["Index"]

                                # Handle both single events and event arrays
                                events = message.get("Events", [message])
                                for event in events:
                                    if "Index" in event:
                                        last_index = max(last_index, event["Index"])
                                    self._pretty_print_event(event)
                            except json.JSONDecodeError as e:
                                self.console.print(f"[red]Error decoding event: {e}")
                                continue

            except (
                requests.exceptions.RequestException,
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
            ) as e:
                self.console.print(f"[red]Error in event stream: {e}")

                if max_retries != -1 and retry_count >= max_retries:
                    self.console.print("[red]Max retry attempts reached. Exiting.")
                    break

                retry_count += 1
                self.console.print(
                    f"[yellow]Retrying connection in {retry_delay} seconds... (Attempt {retry_count})"
                )
                time.sleep(retry_delay)
                continue

            except KeyboardInterrupt:
                self.console.print(
                    "[yellow]Received interrupt signal. Shutting down gracefully..."
                )
                break

    def _pretty_print_event(self, event: Dict):
        """Pretty print job-related events."""
        # Create a rich table for the event
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Field", style="cyan")
        table.add_column("Value", style="yellow")

        # Add basic event info
        table.add_row(
            "Time",
            datetime.fromtimestamp(event.get("Index", 0) / 1000).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        )
        table.add_row("Topic", event.get("Topic", "N/A"))
        table.add_row("Type", event.get("Type", "N/A"))

        # Add event-specific fields based on topic
        payload = event.get("Payload", {})

        if event.get("Topic") == "Job":
            job = payload.get("Job", {})
            table.add_row("Job ID", job.get("ID", "N/A"))
            table.add_row("Status", job.get("Status", "N/A"))

        elif event.get("Topic") == "Evaluation":
            eval_data = payload.get("Evaluation", {})
            table.add_row("Eval ID", eval_data.get("ID", "N/A"))
            table.add_row("Status", eval_data.get("Status", "N/A"))
            table.add_row("Type", eval_data.get("Type", "N/A"))

        elif event.get("Topic") == "Allocation":
            alloc = payload.get("Allocation", {})
            table.add_row("Alloc ID", alloc.get("ID", "N/A"))
            table.add_row("Node ID", alloc.get("NodeID", "N/A"))
            table.add_row("Desired Status", alloc.get("DesiredStatus", "N/A"))
            table.add_row("Client Status", alloc.get("ClientStatus", "N/A"))

            # Add task states if available
            task_states = alloc.get("TaskStates", {})
            for task_name, task_state in task_states.items():
                events = task_state.get("Events", [])
                recent_events = events[-3:] if events else []  # Show last 3 events
                events_str = "\n".join(
                    f"  - {e.get('DisplayMessage', 'N/A')}" for e in recent_events
                )
                table.add_row(
                    f"Task: {task_name}",
                    f"State: {task_state.get('State', 'N/A')}\nRecent Events:\n{events_str}",
                )

        self.console.print(
            Panel(table, title=f"[bold blue]Event {event.get('Index', 'N/A')}")
        )


def main():
    parser = argparse.ArgumentParser(
        description="Coordinate Nomad inundation processing jobs"
    )
    parser.add_argument(
        "--catchment-data", required=True, help="Path to catchment data JSON"
    )
    parser.add_argument("--forecast-path", required=True, help="Path to forecast CSV")
    parser.add_argument("--output-base", required=True, help="Base S3 path for outputs")
    parser.add_argument(
        "--nomad-addr", default="http://localhost:4646", help="Nomad API address"
    )
    parser.add_argument(
        "--monitor", action="store_true", help="Monitor job events after submission"
    )
    args = parser.parse_args()

    with open(args.catchment_data, "r") as f:
        inundate_data = json.load(f)

    coordinator = NomadCoordinator(args.nomad_addr)

    # Submit jobs for each catchment
    for catchment_id in inundate_data["catchments"].keys():
        catchment_data = inundate_data["catchments"][catchment_id]
        catchment_data_path = coordinator.upload_catchment_data(
            catchment_data, args.output_base, catchment_id
        )
        output_path = f"{args.output_base}/catchment_{catchment_id}.tif"

        metadata = {
            "forecast_path": args.forecast_path,
            "output_path": output_path,
            "catchment_data_path": catchment_data_path,
            "window_size": "1024",
        }

        try:
            result = coordinator.submit_job(metadata)
            coordinator.console.print(
                f"[green]Submitted job for catchment {catchment_id}: {result['EvalID']}"
            )
        except requests.exceptions.RequestException as e:
            coordinator.console.print(
                f"[red]Failed to submit job for catchment {catchment_id}: {e}"
            )

    # If monitoring is enabled, start the event stream with job prefix
    if args.monitor:
        coordinator.console.print("\n[yellow]Starting event stream monitor...")
        coordinator.monitor_job("inundation-processor")


if __name__ == "__main__":
    main()
