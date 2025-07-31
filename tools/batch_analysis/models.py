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