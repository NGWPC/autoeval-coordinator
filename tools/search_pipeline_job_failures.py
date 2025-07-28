import argparse
import sys
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError


def parse_time_input(time_str):
    """Parse time input in format YYYY-MM-DD HH:MM or YYYY-MM-DD"""
    try:
        if " " in time_str:
            return datetime.strptime(time_str, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        else:
            return datetime.strptime(time_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        raise ValueError(f"Invalid time format: {time_str}. Use YYYY-MM-DD or YYYY-MM-DD HH:MM")


def retry_with_backoff(func, max_retries=5, base_delay=1):
    """Retry function with exponential backoff for throttling"""
    for attempt in range(max_retries):
        try:
            return func()
        except ClientError as e:
            if e.response["Error"]["Code"] in ["ThrottlingException", "Throttling"]:
                if attempt < max_retries - 1:
                    delay = base_delay * (2**attempt)
                    print(f"Rate limited, retrying in {delay} seconds... (attempt {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                    continue
            raise


def get_pipeline_log_streams(logs_client, log_group, start_time, end_time):
    """Get log streams containing 'pipeline' within the time range"""
    pipeline_streams = []

    def get_streams_page():
        paginator = logs_client.get_paginator("describe_log_streams")
        return paginator.paginate(
            logGroupName=log_group,
            orderBy="LastEventTime",
            descending=True,
            PaginationConfig={"PageSize": 50},  # Smaller page size to reduce rate limiting
        )

    start_timestamp = int(start_time.timestamp() * 1000)
    end_timestamp = int(end_time.timestamp() * 1000)

    page_iterator = retry_with_backoff(get_streams_page)

    all_streams_count = 0
    pipeline_streams_count = 0

    for page in page_iterator:

        def get_page():
            return page

        page_data = retry_with_backoff(get_page)

        for stream in page_data["logStreams"]:
            all_streams_count += 1
            stream_name = stream["logStreamName"]

            # Check if stream name contains 'pipeline'
            if "pipeline" in stream_name.lower():
                pipeline_streams_count += 1

                # Check if stream has events in our time range
                last_event = stream.get("lastEventTimestamp", 0)
                first_event = stream.get("firstEventTimestamp", 0)

                if last_event >= start_timestamp and first_event <= end_timestamp:
                    pipeline_streams.append(stream_name)
                    print(f"Found matching pipeline stream: {stream_name}")

        # Add small delay between pages to be nice to the API
        time.sleep(0.1)

    print(f"Checked {all_streams_count} total streams, found {pipeline_streams_count} pipeline streams")

    return pipeline_streams


def search_failed_jobs(logs_client, log_group, stream_names, start_time, end_time):
    """Search for JobStatus.FAILED messages in the specified streams"""
    failed_messages = []

    start_timestamp = int(start_time.timestamp() * 1000)
    end_timestamp = int(end_time.timestamp() * 1000)

    for stream_name in stream_names:
        print(f"Searching stream: {stream_name}")

        def search_stream():
            paginator = logs_client.get_paginator("filter_log_events")
            return paginator.paginate(
                logGroupName=log_group,
                logStreamNames=[stream_name],
                startTime=start_timestamp,
                endTime=end_timestamp,
                filterPattern="JobStatus.FAILED",
                PaginationConfig={"PageSize": 100},  # Smaller page size
            )

        try:
            page_iterator = retry_with_backoff(search_stream)

            for page in page_iterator:

                def get_page():
                    return page

                page_data = retry_with_backoff(get_page)

                for event in page_data["events"]:
                    timestamp = datetime.fromtimestamp(event["timestamp"] / 1000, tz=timezone.utc)
                    failed_messages.append({"timestamp": timestamp, "stream": stream_name, "message": event["message"]})

                # Small delay between pages
                time.sleep(0.1)

        except Exception as e:
            print(f"Error searching stream {stream_name}: {e}")
            continue

    return failed_messages


def main():
    """
    Search CloudWatch logs for pipeline job failures.

    This script searches the /aws/ec2/nomad-client-linux-test log group for log streams
    containing "pipeline" and then searches those streams for "JobStatus.FAILED" messages
    within a specified time range.

    Usage examples:
      - python search_pipeline_failures.py "2024-01-15" "2024-01-16"
      - python search_pipeline_failures.py "2024-01-15 09:00" "2024-01-15 17:00"

    """

    parser = argparse.ArgumentParser(description="Search for pipeline job failures in CloudWatch logs")
    parser.add_argument("start_time", help="Start time (YYYY-MM-DD or YYYY-MM-DD HH:MM)")
    parser.add_argument("end_time", help="End time (YYYY-MM-DD or YYYY-MM-DD HH:MM)")
    parser.add_argument("--region", default="us-east-1", help="AWS region (default: us-east-1)")

    args = parser.parse_args()

    # Parse time inputs
    try:
        start_time = parse_time_input(args.start_time)
        end_time = parse_time_input(args.end_time)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    if start_time >= end_time:
        print("Error: Start time must be before end time")
        sys.exit(1)

    # Initialize AWS client
    try:
        logs_client = boto3.client("logs", region_name=args.region)
    except Exception as e:
        print(f"Error initializing AWS client: {e}")
        sys.exit(1)

    log_group = "/aws/ec2/nomad-client-linux-test"

    print(f"Searching log group: {log_group}")
    print(f"Time range: {start_time} to {end_time}")
    print()

    # Get pipeline log streams
    print("Finding pipeline log streams...")
    try:
        pipeline_streams = get_pipeline_log_streams(logs_client, log_group, start_time, end_time)
        print(f"Found {len(pipeline_streams)} pipeline streams")
    except Exception as e:
        print(f"Error getting log streams: {e}")
        sys.exit(1)

    if not pipeline_streams:
        print("No pipeline streams found in the specified time range")
        sys.exit(0)

    # Search for failed jobs
    print("\nSearching for JobStatus.FAILED messages...")
    try:
        failed_messages = search_failed_jobs(logs_client, log_group, pipeline_streams, start_time, end_time)
    except Exception as e:
        print(f"Error searching for failed jobs: {e}")
        sys.exit(1)

    # Display results
    print(f"\nFound {len(failed_messages)} failed job messages:")
    print("-" * 80)

    for msg in sorted(failed_messages, key=lambda x: x["timestamp"]):
        print(f"Time: {msg['timestamp']}")
        print(f"Stream: {msg['stream']}")
        print(f"Message: {msg['message']}")
        print("-" * 80)


if __name__ == "__main__":
    main()
