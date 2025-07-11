#!/bin/bash

echo "Nomad server is ready, registering jobs..."

# Debug: Check environment and mounted paths
echo "REPO_ROOT environment variable: ${REPO_ROOT}"
echo "Checking if /repo is mounted:"
ls -la /repo/ 2>/dev/null | head -5 || echo "/repo directory not found or empty"
echo "Contents of /repo/test (if exists):"
ls -la /repo/test/ 2>/dev/null | head -5 || echo "/repo/test directory not found"

# Register all jobs in the job_defs/local directory
JOB_DIR="/nomad/jobs"

if [ ! -d "$JOB_DIR" ]; then
    echo "Job directory $JOB_DIR not found!"
    exit 1
fi

for job_file in "$JOB_DIR"/*.nomad; do
    if [ -f "$job_file" ]; then
        job_name=$(basename "$job_file" .nomad)
        echo "Registering job: $job_name from $job_file"
        
        # Run the nomad job with the repo_root variable
        nomad job run -var="repo_root=${REPO_ROOT}" "$job_file"
        
        if [ $? -eq 0 ]; then
            echo "Successfully registered job: $job_name"
        else
            echo "Failed to register job: $job_name"
        fi
    fi
done

echo "Job registration complete!"
