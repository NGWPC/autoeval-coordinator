"""Data models for batch analysis."""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class DebugConfig:
    """Configuration for pipeline debugging."""

    batch_name: str
    time_range_days: int
    output_dir: str
    pipeline_log_group: str
    job_log_group: str  
    s3_output_root: Optional[str] = None
    generate_html: bool = False
    aoi_list_path: Optional[str] = None


@dataclass
class FailedJobInfo:
    """Information about a failed job."""

    pipeline_log_stream: str
    job_log_stream: str
    error_messages: List[str]
    timestamp: Optional[str] = None


@dataclass
class UniqueErrorInfo:
    """Information about a unique error pattern with occurrence details."""

    error_pattern: str
    error_type: str  # "json_error", "unhandled_exception", etc.
    occurrence_count: int
    first_occurrence_timestamp: str
    first_occurrence_job_stream: str
    first_occurrence_pipeline_stream: str
    sample_raw_message: str
    affected_job_streams: List[str]


@dataclass
class ErrorAnalysisResult:
    """Results from error analysis with both detailed and deduplicated information."""

    failed_jobs: List[FailedJobInfo]
    unique_errors: List[UniqueErrorInfo]
    unhandled_exceptions: List[FailedJobInfo]
    unique_unhandled_exceptions: List[UniqueErrorInfo]