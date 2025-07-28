#!/usr/bin/env python3

import os
import nomad
from urllib.parse import urlparse

# Test script to compare job counting approaches
NOMAD_ADDR = "http://nomad-server-test.test.nextgenwaterprediction.com:4646"
NOMAD_TOKEN = "16fb43ae-aba3-43dc-08d4-4acfe147b524"

def current_allocation_approach(nomad_client):
    """Current approach: count allocations with running/pending status"""
    try:
        allocations = nomad_client.allocations.get_allocations()
        pipeline_allocs = [alloc for alloc in allocations if alloc.get("JobID", "").startswith("pipeline")]
        
        running_count = 0
        for alloc in pipeline_allocs:
            alloc_status = alloc.get("ClientStatus", "")
            if alloc_status in ["running", "pending"]:
                running_count += 1
        
        print(f"Allocation approach: {running_count} running/pending out of {len(pipeline_allocs)} total allocations")
        return running_count
    except Exception as e:
        print(f"Allocation approach failed: {e}")
        return 0

def job_status_approach(nomad_client):
    """Proposed approach: count jobs with active statuses"""
    try:
        jobs = nomad_client.jobs.get_jobs()
        pipeline_jobs = [job for job in jobs if job.get("ID", "").startswith("pipeline")]
        
        running_count = 0
        status_counts = {}
        
        for job in pipeline_jobs:
            job_status = job.get("Status", "")
            status_counts[job_status] = status_counts.get(job_status, 0) + 1
            
            # Count jobs that are active (not finished)
            if job_status in ["running", "pending"]:
                running_count += 1
        
        print(f"Job status approach: {running_count} active out of {len(pipeline_jobs)} total jobs")
        print(f"Status breakdown: {status_counts}")
        return running_count
    except Exception as e:
        print(f"Job status approach failed: {e}")
        return 0

def hybrid_approach(nomad_client):
    """Hybrid approach: count jobs but exclude definitely finished ones"""
    try:
        jobs = nomad_client.jobs.get_jobs()
        pipeline_jobs = [job for job in jobs if job.get("ID", "").startswith("pipeline")]
        
        running_count = 0
        status_counts = {}
        
        for job in pipeline_jobs:
            job_status = job.get("Status", "")
            status_counts[job_status] = status_counts.get(job_status, 0) + 1
            
            # Count jobs that are NOT definitively finished
            if job_status not in ["dead"]:
                running_count += 1
        
        print(f"Hybrid approach: {running_count} non-dead out of {len(pipeline_jobs)} total jobs")
        print(f"Status breakdown: {status_counts}")
        return running_count
    except Exception as e:
        print(f"Hybrid approach failed: {e}")
        return 0

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

    print("Testing different job counting approaches for pipeline jobs:")
    print("=" * 60)
    
    alloc_count = current_allocation_approach(nomad_client)
    print()
    job_count = job_status_approach(nomad_client)
    print()
    hybrid_count = hybrid_approach(nomad_client)
    print()
    
    print("Summary:")
    print(f"  Current (allocations): {alloc_count}")
    print(f"  Job status: {job_count}")
    print(f"  Hybrid (non-dead): {hybrid_count}")

if __name__ == "__main__":
    main()