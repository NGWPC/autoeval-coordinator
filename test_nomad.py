#!/usr/bin/env python3
import json
import requests
import argparse
import os
import base64
from typing import Dict, Any


def test_dispatch(data: Dict, meta: Dict = None) -> bool:
    """Test dispatching a job with given data."""
    if meta is None:
        meta = {
            "forecast_path": "test.csv",
            "output_path": "test.tif",
            "catchment_id": "test-id",
            "window_size": "1024",
        }

    payload_bytes = json.dumps(data).encode("utf-8")
    payload_b64 = base64.b64encode(payload_bytes).decode("utf-8")

    dispatch_payload = {"Meta": meta, "Payload": payload_b64}

    url = "http://localhost:4646/v1/job/inundation-processor/dispatch"
    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(url, json=dispatch_payload, headers=headers)
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        return response.status_code == 200
    except Exception as e:
        print(f"Error: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Test Nomad dispatch with different payload sizes"
    )
    parser.add_argument(
        "--catchment-data", required=True, help="Path to catchment data JSON"
    )
    args = parser.parse_args()

    # Load the full data
    with open(args.catchment_data, "r") as f:
        inundate_data = json.load(f)

    catchment_id = "545b5db0-fc38-54af-9fd8-86e1c1dc4ad5"
    catchment_data = inundate_data["catchments"][catchment_id]

    # Test 1: Minimal test
    print("\nTest 1: Minimal test payload")
    test_dispatch({"test": "data"})

    # Test 2: Just raster paths
    print("\nTest 2: Raster paths only")
    raster_only = {"raster_pair": catchment_data["raster_pair"]}
    test_dispatch(raster_only)

    # Test 3: Single hydrotable entry
    print("\nTest 3: Single hydrotable entry")
    first_hydro_id = next(iter(catchment_data["hydrotable_entries"]))
    single_hydro = {
        "raster_pair": catchment_data["raster_pair"],
        "hydrotable_entries": {
            first_hydro_id: catchment_data["hydrotable_entries"][first_hydro_id]
        },
    }
    test_dispatch(single_hydro)

    # Test 4: Full payload but with real metadata
    print("\nTest 4: Full payload with real metadata")
    meta = {
        "forecast_path": "s3://fimc-data/benchmark/ripple/nwm_return_period_flows_10_yr_cms.csv",
        "output_path": f"s3://fimc-data/benchmark/stac-bench-cat/assets/catchment_{catchment_id}.tif",
        "catchment_id": catchment_id,
        "window_size": "1024",
    }
    test_dispatch(catchment_data, meta)

    # Calculate and print sizes
    print("\nPayload sizes (bytes):")
    print(f"Test 1: {len(json.dumps({'test': 'data'}))}")
    print(f"Test 2: {len(json.dumps(raster_only))}")
    print(f"Test 3: {len(json.dumps(single_hydro))}")
    print(f"Test 4: {len(json.dumps(catchment_data))}")


if __name__ == "__main__":
    main()
