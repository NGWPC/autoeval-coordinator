"""Main orchestrator for batch run analysis."""

import logging
import time
from typing import Any, Dict

from .cloudwatch_analyzer import CloudWatchAnalyzer
from .html_generator import HTMLGenerator
from .models import DebugConfig, FailedJobInfo
from .report_generator import ReportGenerator
from .s3_analyzer import S3MetricsAnalyzer

logger = logging.getLogger(__name__)


class BatchRunAnalyzer:
    """Main orchestrator for batch run analysis."""

    def __init__(self, config: DebugConfig):
        self.config = config
        self.cloudwatch = CloudWatchAnalyzer(config)
        self.s3_analyzer = S3MetricsAnalyzer(config) if config.s3_output_root else None
        self.report_generator = ReportGenerator(config.output_dir)
        self.html_generator = HTMLGenerator(config.output_dir) if config.generate_html else None

    def run_analysis(self) -> Dict[str, Any]:
        """Run complete batch run analysis."""
        logger.info(f"Starting batch run analysis for batch: {self.config.batch_name}")

        results = {
            "batch_name": self.config.batch_name,
            "time_range_days": self.config.time_range_days,
            "analysis_timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
            "reports_generated": [],
        }

        # CloudWatch analysis with optimization
        logger.info("=== CloudWatch Analysis (Optimized) ===")
        error_analysis = self.cloudwatch.find_failed_jobs_optimized()
        unhandled_exceptions_unique = self.cloudwatch.find_unhandled_exceptions_optimized()
        submitted_pipelines = self.cloudwatch.find_submitted_pipelines()

        # Extract data from optimized results
        failed_jobs = error_analysis.failed_jobs
        unique_errors = error_analysis.unique_errors
        
        results["failed_jobs_count"] = len(failed_jobs)
        results["unique_error_patterns_count"] = len(unique_errors)
        results["unhandled_exceptions_count"] = len(unhandled_exceptions_unique)
        results["submitted_pipelines_count"] = len(submitted_pipelines)
        
        # Store unique analysis for reporting
        results["unique_errors"] = unique_errors
        results["unique_unhandled_exceptions"] = unhandled_exceptions_unique

        # AOI list comparison (if provided)
        missing_aois = []
        if self.config.aoi_list_path:
            logger.info("=== AOI List Comparison ===")
            try:
                # Read expected AOIs from file
                with open(self.config.aoi_list_path, 'r') as f:
                    expected_aois = [line.strip() for line in f if line.strip()]
                
                logger.info(f"Loaded {len(expected_aois)} expected AOIs from {self.config.aoi_list_path}")
                
                # Find missing AOIs
                missing_aois = self.cloudwatch.find_missing_pipelines(expected_aois)
                results["missing_aois_count"] = len(missing_aois)
                
            except Exception as e:
                logger.error(f"Failed to process AOI list: {e}")
                results["missing_aois_count"] = 0

        # Generate CloudWatch reports
        if failed_jobs:
            report_file = self.report_generator.generate_failed_jobs_report(failed_jobs)
            results["reports_generated"].append(report_file)

        # Generate unique error pattern reports (new optimized reports)
        if unique_errors:
            report_file = self.report_generator.generate_unique_errors_report(unique_errors)
            results["reports_generated"].append(report_file)

        # Initialize unhandled_exceptions list for HTML generator
        unhandled_exceptions = []
        
        if unhandled_exceptions_unique:
            # Generate unique exceptions report
            report_file = self.report_generator.generate_unique_exceptions_report(unhandled_exceptions_unique)
            results["reports_generated"].append(report_file)
            
            # Also generate legacy format for compatibility
            for unique_exc in unhandled_exceptions_unique:
                unhandled_exceptions.append(
                    FailedJobInfo(
                        pipeline_log_stream=unique_exc.first_occurrence_pipeline_stream,
                        job_log_stream=unique_exc.first_occurrence_job_stream,
                        error_messages=[unique_exc.sample_raw_message]
                    )
                )
            report_file = self.report_generator.generate_unhandled_exceptions_report(unhandled_exceptions)
            results["reports_generated"].append(report_file)

        # Generate missing AOIs report
        if missing_aois:
            report_file = self.report_generator.generate_missing_aois_report(missing_aois, self.config.batch_name)
            results["reports_generated"].append(report_file)

        # S3 metrics analysis
        missing_metrics = []
        empty_metrics = []
        missing_agg = []
        
        if self.s3_analyzer:
            logger.info("=== S3 Metrics Analysis ===")
            missing_metrics = self.s3_analyzer.find_missing_metrics()
            empty_metrics = self.s3_analyzer.find_empty_metrics()
            missing_agg = self.s3_analyzer.find_missing_agg_metrics()

            results["missing_metrics_count"] = len(missing_metrics)
            results["empty_metrics_count"] = len(empty_metrics)
            results["missing_agg_metrics_count"] = len(missing_agg)

            # Generate metrics reports
            metrics_reports = self.report_generator.generate_metrics_reports(
                missing_metrics, empty_metrics, missing_agg
            )
            results["reports_generated"].extend(metrics_reports)

        # Generate summary
        summary_file = self.report_generator.generate_summary_report(results)
        results["reports_generated"].append(summary_file)

        # Generate HTML dashboard if requested
        if self.html_generator:
            logger.info("=== Generating HTML Dashboard ===")
            html_file = self.html_generator.generate_dashboard(
                results,
                failed_jobs,
                unhandled_exceptions,
                missing_metrics if self.s3_analyzer else None,
                empty_metrics if self.s3_analyzer else None,
                missing_agg if self.s3_analyzer else None,
                missing_aois if missing_aois else None,
                unique_errors,
                unhandled_exceptions_unique,
            )
            results["reports_generated"].append(html_file)

        logger.info("=== Analysis Complete ===")
        logger.info(f"Reports generated in: {self.config.output_dir}")
        for report in results["reports_generated"]:
            logger.info(f"  - {report}")

        return results