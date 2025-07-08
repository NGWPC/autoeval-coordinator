#!/bin/bash

echo "Nomad server is ready, registering jobs..."

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
        
        # Run the nomad job with the file
        nomad job run "$job_file"
        
        if [ $? -eq 0 ]; then
            echo "Successfully registered job: $job_name"
        else
            echo "Failed to register job: $job_name"
        fi
    fi
done

echo "Job registration complete!"
