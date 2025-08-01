"""Error pattern extraction utilities for deduplicating and analyzing error messages."""

import json
import re
from typing import Dict, List, Optional, Tuple


class ErrorPatternExtractor:
    """Extracts meaningful error patterns from log messages for deduplication."""

    # Common patterns to normalize in error messages
    NORMALIZATION_PATTERNS = [
        # Remove timestamps (various formats)
        (r'\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}[.\d]*Z?', '[TIMESTAMP]'),
        (r'\d{2}:\d{2}:\d{2}[.\d]*', '[TIME]'),
        
        # Remove job/pipeline specific identifiers
        (r'job-[a-zA-Z0-9-]+', '[JOB_ID]'),
        (r'pipeline-[a-zA-Z0-9-]+', '[PIPELINE_ID]'),
        (r'aoi_name=[^,\]]+', 'aoi_name=[AOI_NAME]'),
        (r'batch_name=[^,\]]+', 'batch_name=[BATCH_NAME]'),
        
        # Remove specific file paths but keep the pattern
        (r'/[a-zA-Z0-9_/.-]+/([^/\s]+\.(py|json|txt|csv|tif|tiff))', r'/[PATH]/\1'),
        (r's3://[a-zA-Z0-9_/.-]+', 's3://[S3_PATH]'),
        
        # Remove specific numbers that might be variable
        (r'\b\d+\.\d+\b', '[NUMBER]'),  # Decimal numbers
        (r'\b\d{4,}\b', '[LARGE_NUMBER]'),  # Large integers (4+ digits)
        
        # Remove memory addresses and object IDs
        (r'0x[a-fA-F0-9]+', '[MEMORY_ADDR]'),
        (r'[a-fA-F0-9]{8,}', '[OBJECT_ID]'),
        
        # Remove temporary file identifiers
        (r'tmp[a-zA-Z0-9_-]+', '[TEMP_FILE]'),
    ]

    def extract_json_error_pattern(self, message: str) -> Tuple[str, str]:
        """
        Extract error pattern from JSON-formatted log messages.
        
        Returns:
            Tuple of (error_pattern, error_type)
        """
        try:
            log_data = json.loads(message)
            
            # Extract the core error message
            error_msg = log_data.get('message', '')
            level = log_data.get('level', '')
            
            # Try to get more specific error info
            if 'exception' in log_data:
                error_type = log_data['exception'].get('type', 'UnknownException')
                error_msg = log_data['exception'].get('message', error_msg)
                pattern = f"{error_type}: {error_msg}"
            elif 'error' in log_data:
                error_type = 'Error'
                error_msg = log_data['error'] if isinstance(log_data['error'], str) else str(log_data['error'])
                pattern = f"Error: {error_msg}"
            else:
                error_type = level.title() if level else 'LogMessage'
                pattern = error_msg
            
            # Normalize the pattern
            normalized_pattern = self._normalize_pattern(pattern)
            
            return normalized_pattern, f"json_{error_type.lower()}"
            
        except json.JSONDecodeError:
            # Not a JSON message, treat as raw text
            return self.extract_raw_error_pattern(message)

    def extract_raw_error_pattern(self, message: str) -> Tuple[str, str]:
        """
        Extract error pattern from raw text error messages.
        
        Returns:
            Tuple of (error_pattern, error_type)
        """
        # Common error type patterns
        error_type_patterns = [
            (r'Traceback.*?(\w+Error: .+)', 'python_exception'),
            (r'(\w+Exception: .+)', 'exception'),
            (r'(ERROR: .+)', 'error'),
            (r'(FATAL: .+)', 'fatal'),
            (r'(FAIL.*?: .+)', 'failure'),
            (r'(\w+Error\b.*)', 'error'),
        ]
        
        # Try to match known error patterns
        for pattern_regex, error_type in error_type_patterns:
            match = re.search(pattern_regex, message, re.IGNORECASE | re.DOTALL)
            if match:
                error_pattern = match.group(1).strip()
                # Take only first few lines to avoid huge stack traces
                lines = error_pattern.split('\n')[:3]
                error_pattern = '\n'.join(lines).strip()
                normalized_pattern = self._normalize_pattern(error_pattern)
                return normalized_pattern, error_type
        
        # Fallback: use first non-empty line as pattern
        lines = [line.strip() for line in message.split('\n') if line.strip()]
        if lines:
            pattern = lines[0][:500]  # Limit length
            normalized_pattern = self._normalize_pattern(pattern)
            return normalized_pattern, 'unhandled_message'
        
        return self._normalize_pattern(message[:200]), 'unknown'

    def _normalize_pattern(self, pattern: str) -> str:
        """Normalize an error pattern by removing variable components."""
        normalized = pattern
        
        for regex_pattern, replacement in self.NORMALIZATION_PATTERNS:
            normalized = re.sub(regex_pattern, replacement, normalized)
        
        # Remove extra whitespace
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        return normalized

    def group_similar_errors(self, error_messages: List[str]) -> Dict[str, List[str]]:
        """
        Group similar error messages by their extracted patterns.
        
        Returns:
            Dictionary mapping error patterns to lists of original messages
        """
        groups = {}
        
        for message in error_messages:
            if message.strip().startswith('{'):
                pattern, _ = self.extract_json_error_pattern(message)
            else:
                pattern, _ = self.extract_raw_error_pattern(message)
            
            if pattern not in groups:
                groups[pattern] = []
            groups[pattern].append(message)
        
        return groups

    def extract_error_signature(self, message: str) -> str:
        """
        Extract a short signature/summary of an error for quick identification.
        
        Returns:
            A short string summarizing the error type and key details
        """
        pattern, error_type = self.extract_json_error_pattern(message) if message.strip().startswith('{') else self.extract_raw_error_pattern(message)
        
        # Create a signature from the first 100 chars of the pattern
        signature = pattern[:100]
        if len(pattern) > 100:
            signature += "..."
        
        return f"[{error_type}] {signature}"