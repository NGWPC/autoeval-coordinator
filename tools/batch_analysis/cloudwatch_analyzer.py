"""CloudWatch Logs analysis for pipeline debugging."""

import logging
import time
from typing import Dict, List, Set

import boto3

from .models import DebugConfig, FailedJobInfo

logger = logging.getLogger(__name__)


class CloudWatchAnalyzer:
    """Handles CloudWatch Logs analysis for pipeline debugging."""

    def __init__(self, config: DebugConfig):
        self.config = config
        self.client = boto3.client("logs")

    def run_query(self, log_group: str, query_string: str) -> List[Dict]:
        """Execute a CloudWatch Logs Insights query and return results."""
        try:
            start_time = int((time.time() - self.config.time_range_days * 86400) * 1000)
            end_time = int(time.time() * 1000)

            logger.info(f"Running query on {log_group}: {query_string[:100]}...")

            start_query_response = self.client.start_query(
                logGroupName=log_group, startTime=start_time, endTime=end_time, queryString=query_string
            )
            query_id = start_query_response["queryId"]

            # Poll for completion
            response = None
            while response is None or response["status"] in ["Running", "Scheduled"]:
                logger.info("Waiting for query to complete...")
                time.sleep(2)
                response = self.client.get_query_results(queryId=query_id)

            logger.info(f"Query completed with {len(response['results'])} results")
            return response["results"]

        except Exception as e:
            logger.error(f"Error running query on {log_group}: {e}")
            return []

    def find_failed_jobs(self) -> List[FailedJobInfo]:
        """Find all failed jobs and extract their error information."""
        logger.info("Step 1: Finding failed jobs...")

        # Query to find failed jobs with job stream names
        failed_jobs_query = f"""
        fields @timestamp, @logStream as pipeline_log_stream
        | parse @message 'Job * ' as job_stream_name
        | filter @logStream like /pipeline\\/dispatch-\\[batch_name={self.config.batch_name}.*\\]/
        | filter @message like /JobStatus.FAILED/
        | sort @timestamp desc
        | display @timestamp, pipeline_log_stream, job_stream_name
        """

        failed_job_results = self.run_query(self.config.pipeline_log_group, failed_jobs_query)

        if not failed_job_results:
            logger.warning("No failed jobs found")
            return []

        logger.info(f"Found {len(failed_job_results)} failed jobs. Analyzing errors...")

        failed_jobs = []
        for job in failed_job_results:
            timestamp = self._get_field_value(job, "@timestamp")
            pipeline_stream = self._get_field_value(job, "pipeline_log_stream")
            job_stream = self._get_field_value(job, "job_stream_name", "").strip()

            if not job_stream or job_stream == "N/A":
                continue

            logger.info(f"Analyzing errors for job: {job_stream}")

            # Query for error messages in the specific job stream
            error_query = f"""
            fields @timestamp, @message
            | filter @logStream = "{job_stream}" and @message like /"level": "ERROR"/
            | sort @timestamp asc
            """

            error_results = self.run_query(self.config.job_log_group, error_query)

            error_messages = []
            if error_results:
                for result in error_results:
                    message = self._get_field_value(result, "@message")
                    if message and message != "Parse Error":
                        error_messages.append(message)
            else:
                error_messages.append("No 'ERROR' level messages found in job log stream")

            failed_jobs.append(
                FailedJobInfo(
                    pipeline_log_stream=pipeline_stream,
                    job_log_stream=job_stream,
                    error_messages=error_messages,
                    timestamp=timestamp,
                )
            )

        return failed_jobs

    def find_unhandled_exceptions(self) -> List[FailedJobInfo]:
        """Find jobs with unhandled exceptions (non-JSON formatted errors)."""
        logger.info("Finding unhandled exceptions...")

        # First get failed jobs
        failed_jobs_query = f"""
        fields @logStream as pipeline_log_stream
        | parse @message 'Job * ' as job_stream_name
        | filter @logStream like /pipeline\\/dispatch-\\[batch_name={self.config.batch_name}.*\\]/
        | filter @message like /JobStatus.FAILED/
        | display pipeline_log_stream, job_stream_name
        """

        failed_job_results = self.run_query(self.config.pipeline_log_group, failed_jobs_query)

        if not failed_job_results:
            return []

        unhandled_exceptions = []
        for job in failed_job_results:
            pipeline_stream = self._get_field_value(job, "pipeline_log_stream")
            job_stream = self._get_field_value(job, "job_stream_name", "").strip()

            if not job_stream or job_stream == "N/A":
                continue

            # Query for non-JSON error messages
            unhandled_error_query = f"""
            fields @timestamp, @message
            | filter @logStream = "{job_stream}"
                and @message not like /^{{/
                and @message like /(?i)traceback|exception|error|fail|fatal/
            | sort @timestamp asc
            """

            unhandled_error_results = self.run_query(self.config.job_log_group, unhandled_error_query)

            error_messages = []
            if unhandled_error_results:
                for result in unhandled_error_results:
                    message = self._get_field_value(result, "@message")
                    if message and message != "Parse Error":
                        error_messages.append(message)
            else:
                error_messages.append("No non-JSON error messages found")

            if error_messages and error_messages != ["No non-JSON error messages found"]:
                unhandled_exceptions.append(
                    FailedJobInfo(
                        pipeline_log_stream=pipeline_stream, job_log_stream=job_stream, error_messages=error_messages
                    )
                )

        return unhandled_exceptions

    def find_submitted_pipelines(self) -> Set[str]:
        """Find all pipelines that were submitted for this batch."""
        logger.info("Finding submitted pipelines...")

        submitted_query = f"""
        fields @logStream
        | filter @logStream like /pipeline\\/dispatch-\\[batch_name={self.config.batch_name}.*\\]/
        | stats count() by @logStream
        """

        results = self.run_query(self.config.pipeline_log_group, submitted_query)

        submitted_streams = set()
        for result in results:
            stream = self._get_field_value(result, "@logStream")
            if stream:
                submitted_streams.add(stream)

        logger.info(f"Found {len(submitted_streams)} submitted pipeline streams")
        return submitted_streams

    def _get_field_value(self, result: Dict, field_name: str, default: str = "N/A") -> str:
        """Extract field value from CloudWatch Logs Insights result."""
        for field in result:
            if field.get("field") == field_name:
                return field.get("value", default)
        return default