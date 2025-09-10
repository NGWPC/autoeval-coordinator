#!/bin/bash

## Nomad Memory Monitoring and Garbage Collection Script

## This script monitors Nomad memory usage and triggers garbage collection when needed.
# Set the memory threshold in GiB at which garbage collection should trigger
MEMORY_THRESHOLD_GIB=6

MEMORY_THRESHOLD_BYTES=$(( MEMORY_THRESHOLD_GIB * 1024 * 1024 * 1024 ))

get_nomad_alloc_bytes() {
    local metrics_json
    metrics_json=$(nomad operator metrics -json 2>&1 | grep -v "Unable to retrieve credentials")
    if [ ${PIPESTATUS[0]} -ne 0 ]; then
        echo "Error: 'nomad operator metrics' command failed" >&2
        return 1
    fi
    # alloc_bytes indicates the number of bytes currently allocated by the nomad server
    local alloc_bytes
    alloc_bytes=$(echo "$metrics_json" | jq -r '.Gauges[] | select(.Name == "nomad.runtime.alloc_bytes") | .Value')
    
    if [[ -z "$alloc_bytes" || "$alloc_bytes" == "null" ]]; then
        echo "Error: Could not parse 'nomad.runtime.alloc_bytes' from Nomad." >&2
        return 2
    fi
    echo "$alloc_bytes"
}

start_nomad_gc() {
    # Set default log file path if not provided
    local log_file="${1:-nomad_memory_usage.log}"
    echo "Starting Nomad garbage collection background process..."
    echo "Memory usage logs will be written to: $log_file"
    
    (
        export NOMAD_TOKEN="${NOMAD_TOKEN}"
        export NOMAD_ADDR="${NOMAD_ADDR:-http://nomad-server-test.test.nextgenwaterprediction.com:4646}"
        
        while true; do
            local current_alloc_bytes
            current_alloc_bytes=$(get_nomad_alloc_bytes)
            local exit_code=$?

            if [[ $exit_code -eq 0 ]]; then
                # Write memory usage to log file
                echo "$(date): Current alloc_bytes: $((current_alloc_bytes / 1024 / 1024 / 1024))GB | Threshold: $((MEMORY_THRESHOLD_BYTES / 1024 / 1024 / 1024))GB" >> "$log_file"
                
                # Direct comparison: trigger if allocated bytes are greater than or equal to the threshold
                if [[ $current_alloc_bytes -ge $MEMORY_THRESHOLD_BYTES ]]; then
                    echo "$(date): Memory usage ($((current_alloc_bytes / 1024 / 1024 / 1024))GB) has exceeded the threshold ($((MEMORY_THRESHOLD_BYTES / 1024 / 1024 / 1024))GB). Running 'nomad system gc'..." >> "$log_file"
                    nomad system gc
                fi
            else
                echo "$(date): Warning - Could not check Nomad memory usage (exit code: $exit_code)" >> "$log_file"
            fi
            sleep 120
        done
    ) &

    NOMAD_GC_PID=$!
    echo "Nomad GC background process started with PID: $NOMAD_GC_PID"
    
    # Function to cleanup background process on script exit
    cleanup_nomad_gc() {
        if [[ -n "$NOMAD_GC_PID" ]]; then
            echo "Stopping Nomad GC background process (PID: $NOMAD_GC_PID)..."
            kill $NOMAD_GC_PID 2>/dev/null
        fi
    }
    
    # Set trap to cleanup on script exit
    trap cleanup_nomad_gc EXIT
}

# If script is run directly (not sourced), start the memory monitoring
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    echo "=== Nomad Memory Monitor ==="
    start_nomad_gc
    
    # Keep the script running
    echo "Memory monitoring active. Press Ctrl+C to stop."
    wait $NOMAD_GC_PID
fi
