"""CSV report generation for batch analysis results."""

import csv
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Set

from .models import FailedJobInfo, UniqueErrorInfo

logger = logging.getLogger(__name__)


class ReportGenerator:
    """Generates formatted CSV reports from analysis results."""

    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate_failed_jobs_report(self, failed_jobs: List[FailedJobInfo]) -> str:
        """Generate CSV report of failed jobs with errors."""
        output_file = self.output_dir / "failed_jobs_errors.csv"

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["timestamp", "pipeline_log_stream", "job_log_stream", "error_messages"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for job in failed_jobs:
                writer.writerow(
                    {
                        "timestamp": job.timestamp or "",
                        "pipeline_log_stream": job.pipeline_log_stream,
                        "job_log_stream": job.job_log_stream,
                        "error_messages": "\n\n".join(job.error_messages),
                    }
                )

        logger.info(f"Generated failed jobs report: {output_file}")
        return str(output_file)

    def generate_unhandled_exceptions_report(self, exceptions: List[FailedJobInfo]) -> str:
        """Generate CSV report of unhandled exceptions."""
        output_file = self.output_dir / "unhandled_exceptions.csv"

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["pipeline_log_stream", "job_log_stream", "unhandled_error_messages"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for job in exceptions:
                writer.writerow(
                    {
                        "pipeline_log_stream": job.pipeline_log_stream,
                        "job_log_stream": job.job_log_stream,
                        "unhandled_error_messages": "\n\n".join(job.error_messages),
                    }
                )

        logger.info(f"Generated unhandled exceptions report: {output_file}")
        return str(output_file)

    def generate_missing_pipelines_report(self, submitted: Set[str], expected: Set[str]) -> str:
        """Generate report of missing pipeline executions."""
        output_file = self.output_dir / "missing_pipelines.txt"

        missing = expected - submitted

        with open(output_file, "w") as f:
            f.write(f"Missing Pipeline Executions Report\n")
            f.write(f"====================================\n\n")
            f.write(f"Expected pipelines: {len(expected)}\n")
            f.write(f"Submitted pipelines: {len(submitted)}\n")
            f.write(f"Missing pipelines: {len(missing)}\n\n")

            if missing:
                f.write("Missing pipeline streams:\n")
                for stream in sorted(missing):
                    f.write(f"  {stream}\n")
            else:
                f.write("All expected pipelines were submitted.\n")

        logger.info(f"Generated missing pipelines report: {output_file}")
        return str(output_file)

    def generate_missing_aois_report(self, missing_aois: List[str], batch_name: str) -> str:
        """Generate CSV report of AOIs that have no corresponding pipeline logs."""
        output_file = self.output_dir / "missing_aois.csv"

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["aoi_id", "batch_name", "expected_log_stream_pattern"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for aoi_id in missing_aois:
                expected_pattern = f"pipeline/dispatch-[batch_name={batch_name},aoi_name={aoi_id}]"
                writer.writerow(
                    {
                        "aoi_id": aoi_id,
                        "batch_name": batch_name,
                        "expected_log_stream_pattern": expected_pattern,
                    }
                )

        logger.info(f"Generated missing AOIs report: {output_file}")
        return str(output_file)

    def generate_metrics_reports(
        self, missing_metrics: List[Dict], empty_metrics: List[Dict], missing_agg: List[Dict]
    ) -> List[str]:
        """Generate reports for metrics file issues."""
        reports = []

        # Missing metrics report
        if missing_metrics:
            output_file = self.output_dir / "missing_metrics.csv"
            with open(output_file, "w", newline="") as csvfile:
                fieldnames = ["directory", "issue"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(missing_metrics)
            reports.append(str(output_file))
            logger.info(f"Generated missing metrics report: {output_file}")

        # Empty metrics report
        if empty_metrics:
            output_file = self.output_dir / "empty_metrics.csv"
            with open(output_file, "w", newline="") as csvfile:
                fieldnames = ["file", "line_count", "issue"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(empty_metrics)
            reports.append(str(output_file))
            logger.info(f"Generated empty metrics report: {output_file}")

        # Missing agg_metrics report
        if missing_agg:
            output_file = self.output_dir / "missing_agg_metrics.csv"
            with open(output_file, "w", newline="") as csvfile:
                fieldnames = ["directory", "expected_file", "issue"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(missing_agg)
            reports.append(str(output_file))
            logger.info(f"Generated missing agg_metrics report: {output_file}")

        return reports

    def generate_unique_errors_report(self, unique_errors: List[UniqueErrorInfo]) -> str:
        """Generate CSV report of unique error patterns with occurrence counts."""
        output_file = self.output_dir / "unique_error_patterns.csv"

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = [
                "error_pattern", 
                "error_type", 
                "occurrence_count", 
                "first_occurrence_timestamp",
                "first_occurrence_job_stream",
                "first_occurrence_pipeline_stream",
                "affected_job_streams_count",
                "sample_raw_message"
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for error in unique_errors:
                writer.writerow({
                    "error_pattern": error.error_pattern,
                    "error_type": error.error_type,
                    "occurrence_count": error.occurrence_count,
                    "first_occurrence_timestamp": error.first_occurrence_timestamp,
                    "first_occurrence_job_stream": error.first_occurrence_job_stream,
                    "first_occurrence_pipeline_stream": error.first_occurrence_pipeline_stream,
                    "affected_job_streams_count": len(error.affected_job_streams),
                    "sample_raw_message": error.sample_raw_message[:500] + "..." if len(error.sample_raw_message) > 500 else error.sample_raw_message
                })

        logger.info(f"Generated unique error patterns report: {output_file}")
        return str(output_file)

    def generate_unique_exceptions_report(self, unique_exceptions: List[UniqueErrorInfo]) -> str:
        """Generate CSV report of unique unhandled exception patterns."""
        output_file = self.output_dir / "unique_unhandled_exceptions.csv"

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = [
                "exception_pattern", 
                "exception_type", 
                "occurrence_count", 
                "first_occurrence_timestamp",
                "first_occurrence_job_stream",
                "first_occurrence_pipeline_stream",
                "affected_job_streams_count",
                "sample_raw_message"
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for exception in unique_exceptions:
                writer.writerow({
                    "exception_pattern": exception.error_pattern,
                    "exception_type": exception.error_type,
                    "occurrence_count": exception.occurrence_count,
                    "first_occurrence_timestamp": exception.first_occurrence_timestamp,
                    "first_occurrence_job_stream": exception.first_occurrence_job_stream,
                    "first_occurrence_pipeline_stream": exception.first_occurrence_pipeline_stream,
                    "affected_job_streams_count": len(exception.affected_job_streams),
                    "sample_raw_message": exception.sample_raw_message[:500] + "..." if len(exception.sample_raw_message) > 500 else exception.sample_raw_message
                })

        logger.info(f"Generated unique unhandled exceptions report: {output_file}")
        return str(output_file)

    def generate_summary_report(self, analysis_results: Dict[str, Any]) -> str:
        """Generate JSON summary of all analysis results."""
        output_file = self.output_dir / "debug_summary.json"

        with open(output_file, "w") as f:
            json.dump(analysis_results, f, indent=2, default=str)

        logger.info(f"Generated summary report: {output_file}")
        return str(output_file)