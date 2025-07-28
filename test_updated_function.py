#!/usr/bin/env python3

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "tools"))

from submit_huc_batch import get_running_pipeline_jobs
import nomad
from urllib.parse import urlparse

# Test the updated function
NOMAD_ADDR = "http://nomad-server-test.test.nextgenwaterprediction.com:4646"
NOMAD_TOKEN = "16fb43ae-aba3-43dc-08d4-4acfe147b524"

def main():
    # Initialize Nomad client
    parsed = urlparse(NOMAD_ADDR)
    nomad_client = nomad.Nomad(
        host=parsed.hostname,
        port=parsed.port or 4646,
        verify=False,
        token=NOMAD_TOKEN,
        namespace="default",
    )

    print("Testing updated get_running_pipeline_jobs() function:")
    print("=" * 50)
    
    # Test with debug logging enabled
    import logging
    logging.basicConfig(level=logging.DEBUG)
    
    count = get_running_pipeline_jobs(nomad_client)
    print(f"Active pipeline jobs: {count}")

if __name__ == "__main__":
    main()