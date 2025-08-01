#!/bin/bash
 
## A script to run batches in succession for all the evals to be run. 

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INPUTS_DIR="$SCRIPT_DIR/../inputs"

declare -a RESOLUTIONS=("10" "5" "3")

declare -a COLLECTIONS=("ble-collection" "nws-fim-collection" "ripple-fim-collection" "usgs-fim-collection")

# Generate timestamp in format: YYYY-MM-DD-HH (e.g., 2025-08-01-14 for 2PM on Aug 1, 2025)
TIMESTAMP=$(date +"%Y-%m-%d-%H")

echo "=== Autoeval Batch Processing Script ==="
echo "Processing ${#RESOLUTIONS[@]} resolutions x ${#COLLECTIONS[@]} collections"
echo "Timestamp: $TIMESTAMP"
echo ""

confirm_command() {
    local command="$1"
    echo "About to execute:"
    echo "$command"
    echo ""
    read -p "Proceed? (y/n): " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Skipping command..."
        return 1
    fi
    return 0
}

for resolution in "${RESOLUTIONS[@]}"; do
    echo "=== Processing Resolution: ${resolution}m ==="
    
    # Define resolution-specific parameters
    batch_name="fim100_huc12_${resolution}m_${TIMESTAMP}"
    output_root="s3://fimc-data/autoeval/batches/fim100_huc12_${resolution}m_non_calibrated/"
    hand_index_path="s3://fimc-data/autoeval/hand_output_indices/fim100_huc12_${resolution}m_index/"
    
    # loop over collections
    for collection in "${COLLECTIONS[@]}"; do
        echo "--- Processing Collection: $collection ---"
        
        # Define collection-specific parameters
        item_list_file="$INPUTS_DIR/${collection}.txt"
        
        # Check if the input file exists
        if [[ ! -f "$item_list_file" ]]; then
            echo "Error: Input file $item_list_file not found!"
            continue
        fi
        
        # Build the submit_stac_batch.py command
        submit_cmd="python ./tools/submit_stac_batch.py --batch_name $batch_name --output_root $output_root --hand_index_path $hand_index_path --benchmark_sources \"$collection\" --item_list $item_list_file --wait_seconds 10 --max_pipelines 150" # Scaling tests showed you shouldn't go above 150 pipelines with current Nomad deploy.
        
        # Get user confirmation and execute
        if confirm_command "$submit_cmd"; then
            echo "Executing submit_stac_batch.py..."
            eval $submit_cmd
            if [[ $? -ne 0 ]]; then
                echo "Error: submit_stac_batch.py failed for $collection"
                read -p "Continue with next collection? (y/n): " -n 1 -r
                echo ""
                [[ ! $REPLY =~ ^[Yy]$ ]] && exit 1
            fi
        else
            continue
        fi
        
        # Purge dispatch jobs after each collection
        echo "--- Purging Dispatch Jobs after $collection ---"
        purge_cmd="python tools/purge_dispatch_jobs.py"
        
        if confirm_command "$purge_cmd"; then
            echo "Executing purge_dispatch_jobs.py..."
            eval $purge_cmd
            if [[ $? -ne 0 ]]; then
                echo "Warning: purge_dispatch_jobs.py failed"
            fi
        fi
        
        echo ""
    done
    
    # After processing all collections for this resolution, run make_master_metrics
    echo "--- Creating Master Metrics for ${resolution}m ---"
    master_metrics_cmd="python ./tools/make_master_metrics.py $output_root --hand-version \"fim100_huc12\" --resolution \"$resolution\""
    
    if confirm_command "$master_metrics_cmd"; then
        echo "Executing make_master_metrics.py..."
        eval $master_metrics_cmd
        if [[ $? -ne 0 ]]; then
            echo "Error: make_master_metrics.py failed for ${resolution}m"
            read -p "Continue with next resolution? (y/n): " -n 1 -r
            echo ""
            [[ ! $REPLY =~ ^[Yy]$ ]] && exit 1
        fi
    fi
    
    # Refresh AWS credentials before batch analysis
    echo "--- Refreshing AWS Credentials ---"
    echo "Configuring AWS SSO..."
    aws configure sso
    if [[ $? -ne 0 ]]; then
        echo "Error: AWS SSO configuration failed"
        read -p "Continue anyway? (y/n): " -n 1 -r
        echo ""
        [[ ! $REPLY =~ ^[Yy]$ ]] && exit 1
    fi
    
    if [[ -f "./update_aws_creds.sh" ]]; then
        echo "Running update_aws_creds.sh..."
        ./update_aws_creds.sh
        if [[ $? -ne 0 ]]; then
            echo "Warning: update_aws_creds.sh failed"
        fi
    else
        echo "Warning: update_aws_creds.sh not found, skipping"
    fi
    
    # Generate batch analysis reports
    echo "--- Generating Batch Reports for ${resolution}m ---"
    reports_output_dir="../reports/$batch_name"
    reports_cmd="python tools/batch_run_reports.py --batch_name $batch_name --output_dir $reports_output_dir --pipeline_log_group /aws/ec2/nomad-client-linux-test --job_log_group /aws/ec2/nomad-client-linux-test --s3_output_root $output_root --aoi_list $item_list_file --html"
    
    if confirm_command "$reports_cmd"; then
        echo "Executing batch_run_reports.py..."
        eval $reports_cmd
        if [[ $? -ne 0 ]]; then
            echo "Warning: batch_run_reports.py failed for ${resolution}m"
        else
            echo "Reports generated at: $reports_output_dir"
            echo "View dashboard: $reports_output_dir/batch_analysis_dashboard.html"
        fi
    fi
    
    echo "=== Completed Resolution: ${resolution}m ==="
    echo ""
done
