#!/usr/bin/env python3
import json
import requests
import argparse
import os
import boto3
from typing import Dict, Any
from urllib.parse import urlparse


class NomadCoordinator:
    def __init__(self, nomad_addr: str = "http://localhost:4646"):
        self.nomad_addr = nomad_addr
        self.headers = {"Content-Type": "application/json"}
        self.s3_client = boto3.client("s3")

    def upload_catchment_data(
        self, catchment_data: dict, output_base: str, catchment_id: str
    ) -> str:
        """Upload catchment data to S3 and return the URL."""
        # Parse the output base to get bucket and prefix
        parsed = urlparse(output_base)
        bucket = parsed.netloc
        key = os.path.join(
            parsed.path.lstrip("/"), "temp", f"catchment_{catchment_id}.json"
        )

        # Upload the data
        self.s3_client.put_object(
            Bucket=bucket, Key=key, Body=json.dumps(catchment_data).encode("utf-8")
        )

        return f"s3://{bucket}/{key}"

    def submit_job(self, job_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Submit a parameterized job to Nomad."""
        url = f"{self.nomad_addr}/v1/job/inundation-processor/dispatch"
        dispatch_payload = {"Meta": job_metadata}

        print(f"Submitting job with metadata:\n{json.dumps(job_metadata, indent=2)}")

        response = requests.post(url, json=dispatch_payload, headers=self.headers)
        response.raise_for_status()
        return response.json()


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

    args = parser.parse_args()

    # Load catchment data
    with open(args.catchment_data, "r") as f:
        inundate_data = json.load(f)

    coordinator = NomadCoordinator(args.nomad_addr)

    # Submit job for each catchment
    for catchment_id in inundate_data["catchments"].keys():
        # Extract catchment data and upload to S3
        catchment_data = inundate_data["catchments"][catchment_id]
        catchment_data_path = coordinator.upload_catchment_data(
            catchment_data, args.output_base, catchment_id
        )

        output_path = f"{args.output_base}/catchment_{catchment_id}.tif"

        # Prepare job metadata
        metadata = {
            "forecast_path": args.forecast_path,
            "output_path": output_path,
            "catchment_id": catchment_id,
            "catchment_data_path": catchment_data_path,
            "window_size": "1024",
        }

        # Submit job
        try:
            result = coordinator.submit_job(metadata)
            print(f"Submitted job for catchment {catchment_id}: {result['EvalID']}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to submit job for catchment {catchment_id}: {e}")


if __name__ == "__main__":
    main()
