import argparse
import logging

from batch_analysis import BatchRunAnalyzer, DebugConfig

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def main():
    """
    Batch run analysis script that generates reports for analyzing pipeline batch runs,
    focusing on failures and missing outputs (metrics/agg_metrics files).
    """

    parser = argparse.ArgumentParser(description="Generate comprehensive reports for pipeline batch run analysis")

    # Required arguments
    parser.add_argument("--batch_name", required=True, help="Name of the batch to analyze (e.g., 'fim100_huc12_10m')")
    parser.add_argument("--output_dir", required=True, help="Directory to save generated reports")
    parser.add_argument("--pipeline_log_group", required=True, help="CloudWatch log group for pipeline logs")
    parser.add_argument("--job_log_group", required=True, help="CloudWatch log group for individual job logs")

    # Optional arguments
    parser.add_argument(
        "--time_range_days", type=int, default=7, help="Number of days to look back for logs (default: 7)"
    )
    parser.add_argument("--s3_output_root", help="S3 root path for pipeline outputs (for metrics analysis)")
    parser.add_argument("--html", action="store_true", help="Generate HTML dashboard in addition to CSV reports")
    parser.add_argument("--aoi_list", help="Path to AOI list file (same format as used by submit_stac_batch.py) for missing pipeline detection")

    args = parser.parse_args()

    # Validate arguments
    if args.s3_output_root and not args.s3_output_root.startswith("s3://"):
        parser.error("s3_output_root must start with 's3://'")

    # Create config
    config = DebugConfig(
        batch_name=args.batch_name,
        time_range_days=args.time_range_days,
        output_dir=args.output_dir,
        pipeline_log_group=args.pipeline_log_group,
        job_log_group=args.job_log_group,
        s3_output_root=args.s3_output_root,
        generate_html=args.html,
        aoi_list_path=args.aoi_list,
    )

    try:
        # Run analysis
        analyzer = BatchRunAnalyzer(config)
        results = analyzer.run_analysis()

        # Print summary
        print(f"\nBatch Run Analysis Complete!")
        print(f"Batch: {results['batch_name']}")
        print(f"Failed jobs: {results['failed_jobs_count']}")
        print(f"Unhandled exceptions: {results['unhandled_exceptions_count']}")
        print(f"Submitted pipelines: {results['submitted_pipelines_count']}")

        if "missing_aois_count" in results:
            print(f"Missing AOIs (no logs): {results['missing_aois_count']}")

        if "missing_metrics_count" in results:
            print(f"Missing metrics files: {results['missing_metrics_count']}")
            print(f"Empty/invalid metrics files: {results['empty_metrics_count']}")
            print(f"Missing agg_metrics files: {results['missing_agg_metrics_count']}")

        print(f"\nReports saved to: {config.output_dir}")

        if config.generate_html:
            print(f"📊 View the interactive dashboard: {config.output_dir}/batch_analysis_dashboard.html")

    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        raise


if __name__ == "__main__":
    main()
