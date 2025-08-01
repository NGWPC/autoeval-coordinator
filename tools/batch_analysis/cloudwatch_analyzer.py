"""CloudWatch Logs analysis for pipeline debugging."""

import logging
import re
import time
from collections import defaultdict
from typing import Dict, List, Set

import boto3

from .error_patterns import ErrorPatternExtractor
from .models import DebugConfig, ErrorAnalysisResult, FailedJobInfo, UniqueErrorInfo

logger = logging.getLogger(__name__)


class CloudWatchAnalyzer:
    """Handles CloudWatch Logs analysis for pipeline debugging."""

    def __init__(self, config: DebugConfig):
        self.config = config
        # Use cloudwatch profile for CloudWatch
        session = boto3.Session(profile_name="cloudwatch")
        self.client = session.client("logs")
        self.error_extractor = ErrorPatternExtractor()

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

    def find_missing_pipelines(self, expected_aois: List[str]) -> List[str]:
        """Find AOIs that were expected but have no corresponding pipeline log streams."""
        logger.info(f"Checking for missing pipelines from {len(expected_aois)} expected AOIs...")
        
        # Get all submitted pipeline streams
        submitted_streams = self.find_submitted_pipelines()
        
        # Extract AOI names from log stream names
        # Pattern: pipeline/dispatch-[batch_name=X,aoi_name=Y]
        pattern = r'pipeline/dispatch-\[batch_name=' + re.escape(self.config.batch_name) + r',aoi_name=([^\]]+)\]'
        
        actual_aois = set()
        for stream in submitted_streams:
            match = re.search(pattern, stream)
            if match:
                aoi_name = match.group(1)
                actual_aois.add(aoi_name)
        
        # Find missing AOIs
        expected_aois_set = set(expected_aois)
        missing_aois = expected_aois_set - actual_aois
        
        logger.info(f"Expected AOIs: {len(expected_aois_set)}")
        logger.info(f"Found AOIs with logs: {len(actual_aois)}")
        logger.info(f"Missing AOIs: {len(missing_aois)}")
        
        return sorted(list(missing_aois))

    def _get_field_value(self, result: Dict, field_name: str, default: str = "N/A") -> str:
        """Extract field value from CloudWatch Logs Insights result."""
        for field in result:
            if field.get("field") == field_name:
                return field.get("value", default)
        return default

    def find_failed_jobs_optimized(self) -> ErrorAnalysisResult:
        """
        Find failed jobs and analyze errors with deduplication and first-occurrence optimization.
        This replaces the N+1 query pattern with efficient aggregated queries.
        """
        logger.info("Step 1: Finding failed jobs with optimized error analysis...")

        # Single query to get all failed jobs
        failed_jobs_query = f"""
        fields @timestamp, @logStream as pipeline_log_stream
        | parse @message 'Job * ' as job_stream_name  
        | filter @logStream like /pipeline\\/dispatch-\\[batch_name={self.config.batch_name}.*\\]/
        | filter @message like /JobStatus.FAILED/
        | sort @timestamp desc
        """

        failed_job_results = self.run_query(self.config.pipeline_log_group, failed_jobs_query)

        if not failed_job_results:
            logger.warning("No failed jobs found")
            return ErrorAnalysisResult([], [], [], [])

        logger.info(f"Found {len(failed_job_results)} failed jobs. Running bulk error analysis...")

        # Extract job stream names
        job_streams = []
        job_stream_to_pipeline = {}
        job_stream_to_timestamp = {}
        
        for job in failed_job_results:
            pipeline_stream = self._get_field_value(job, "pipeline_log_stream")
            job_stream = self._get_field_value(job, "job_stream_name", "").strip()
            timestamp = self._get_field_value(job, "@timestamp")
            
            if job_stream and job_stream != "N/A":
                job_streams.append(job_stream)
                job_stream_to_pipeline[job_stream] = pipeline_stream
                job_stream_to_timestamp[job_stream] = timestamp

        if not job_streams:
            logger.warning("No valid job streams found")
            return ErrorAnalysisResult([], [], [], [])

        # Bulk query for all error messages across all failed jobs
        # Process in chunks to avoid query size limits
        all_error_results = []
        chunk_size = 20
        
        for i in range(0, len(job_streams), chunk_size):
            chunk = job_streams[i:i + chunk_size]
            job_streams_filter = " or ".join([f'@logStream = "{stream}"' for stream in chunk])
            
            bulk_errors_query = f"""
            fields @timestamp, @logStream, @message
            | filter ({job_streams_filter}) and @message like /"level": "ERROR"/
            | sort @timestamp asc
            """

            chunk_results = self.run_query(self.config.job_log_group, bulk_errors_query)
            all_error_results.extend(chunk_results)

        # Group errors by job stream and extract patterns
        job_errors = defaultdict(list)
        error_patterns = defaultdict(list)
        
        for result in all_error_results:
            job_stream = self._get_field_value(result, "@logStream")
            message = self._get_field_value(result, "@message")
            timestamp = self._get_field_value(result, "@timestamp")
            
            if message and message != "Parse Error":
                job_errors[job_stream].append(message)
                
                # Extract error pattern for deduplication
                pattern, error_type = self.error_extractor.extract_json_error_pattern(message)
                
                error_patterns[pattern].append({
                    'job_stream': job_stream,
                    'pipeline_stream': job_stream_to_pipeline.get(job_stream, 'unknown'),
                    'timestamp': timestamp,
                    'raw_message': message,
                    'error_type': error_type
                })

        # Create detailed failed job list
        failed_jobs = []
        for job_stream in job_streams:
            errors = job_errors.get(job_stream, ["No 'ERROR' level messages found in job log stream"])
            failed_jobs.append(
                FailedJobInfo(
                    pipeline_log_stream=job_stream_to_pipeline[job_stream],
                    job_log_stream=job_stream,
                    error_messages=errors,
                    timestamp=job_stream_to_timestamp.get(job_stream)
                )
            )

        # Create unique error analysis
        unique_errors = []
        for pattern, occurrences in error_patterns.items():
            # Sort by timestamp to get first occurrence
            occurrences.sort(key=lambda x: x['timestamp'])
            first_occurrence = occurrences[0]
            
            unique_errors.append(
                UniqueErrorInfo(
                    error_pattern=pattern,
                    error_type=first_occurrence['error_type'],
                    occurrence_count=len(occurrences),
                    first_occurrence_timestamp=first_occurrence['timestamp'],
                    first_occurrence_job_stream=first_occurrence['job_stream'],
                    first_occurrence_pipeline_stream=first_occurrence['pipeline_stream'],
                    sample_raw_message=first_occurrence['raw_message'],
                    affected_job_streams=[occ['job_stream'] for occ in occurrences]
                )
            )

        # Sort unique errors by occurrence count (most common first)
        unique_errors.sort(key=lambda x: x.occurrence_count, reverse=True)

        logger.info(f"Analyzed {len(failed_jobs)} failed jobs, found {len(unique_errors)} unique error patterns")

        return ErrorAnalysisResult(failed_jobs, unique_errors, [], [])

    def find_unhandled_exceptions_optimized(self) -> List[UniqueErrorInfo]:
        """
        Find unhandled exceptions with deduplication and first-occurrence optimization.
        """
        logger.info("Finding unhandled exceptions with optimization...")

        # Get failed job streams first
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

        job_streams = []
        job_stream_to_pipeline = {}
        
        for job in failed_job_results:
            pipeline_stream = self._get_field_value(job, "pipeline_log_stream")
            job_stream = self._get_field_value(job, "job_stream_name", "").strip()
            
            if job_stream and job_stream != "N/A":
                job_streams.append(job_stream)
                job_stream_to_pipeline[job_stream] = pipeline_stream

        if not job_streams:
            return []

        # Bulk query for unhandled exceptions in chunks
        all_unhandled_results = []
        chunk_size = 20
        
        for i in range(0, len(job_streams), chunk_size):
            chunk = job_streams[i:i + chunk_size]
            job_streams_filter = " or ".join([f'@logStream = "{stream}"' for stream in chunk])
            
            unhandled_query = f"""
            fields @timestamp, @logStream, @message
            | filter ({job_streams_filter})
                and @message not like /^{{/
                and @message like /(?i)traceback|exception|error|fail|fatal/
            | sort @timestamp asc
            """

            chunk_results = self.run_query(self.config.job_log_group, unhandled_query)
            all_unhandled_results.extend(chunk_results)

        # Group by error patterns
        error_patterns = defaultdict(list)
        
        for result in all_unhandled_results:
            job_stream = self._get_field_value(result, "@logStream")
            message = self._get_field_value(result, "@message")
            timestamp = self._get_field_value(result, "@timestamp")
            
            if message and message != "Parse Error":
                pattern, error_type = self.error_extractor.extract_raw_error_pattern(message)
                
                error_patterns[pattern].append({
                    'job_stream': job_stream,
                    'pipeline_stream': job_stream_to_pipeline.get(job_stream, 'unknown'),
                    'timestamp': timestamp,
                    'raw_message': message,
                    'error_type': error_type
                })

        # Create unique unhandled exceptions
        unique_exceptions = []
        for pattern, occurrences in error_patterns.items():
            occurrences.sort(key=lambda x: x['timestamp'])
            first_occurrence = occurrences[0]
            
            unique_exceptions.append(
                UniqueErrorInfo(
                    error_pattern=pattern,
                    error_type=first_occurrence['error_type'],
                    occurrence_count=len(occurrences),
                    first_occurrence_timestamp=first_occurrence['timestamp'],
                    first_occurrence_job_stream=first_occurrence['job_stream'],
                    first_occurrence_pipeline_stream=first_occurrence['pipeline_stream'],
                    sample_raw_message=first_occurrence['raw_message'],
                    affected_job_streams=[occ['job_stream'] for occ in occurrences]
                )
            )

        unique_exceptions.sort(key=lambda x: x.occurrence_count, reverse=True)
        
        logger.info(f"Found {len(unique_exceptions)} unique unhandled exception patterns")
        return unique_exceptions