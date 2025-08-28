import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import fsspec
import pandas as pd


def setup_logging(log_file: str) -> logging.Logger:
    """
    Set up logging configuration.
    
    Args:
        log_file: Path to the log file
        
    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_format)
    
    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    file_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    
    # Add handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger


def find_agg_metrics_files(s3_prefix: str, batch: str, aws_profile: str = "fimc-data") -> List[str]:
    """
    Find all agg_metrics.csv files under the batch directory.
    
    Args:
        s3_prefix: S3 prefix path (e.g., "s3://fimc-data/autoeval/batches/")
        batch: Batch directory name (e.g., "fim100_huc12_5m_non_calibrated")
        aws_profile: AWS profile to use for S3 access
        
    Returns:
        List of S3 paths to agg_metrics.csv files
    """
    # Construct full batch path
    batch_path = f"{s3_prefix.rstrip('/')}/{batch}"
    
    # Initialize S3 filesystem
    fs = fsspec.filesystem("s3", profile=aws_profile)
    
    agg_metrics_files = []
    
    try:
        # List all run directories under the batch
        batch_items = fs.ls(batch_path, detail=True)
        
        for item in batch_items:
            if item["type"] == "directory":
                run_dir = item["name"]
                run_name = run_dir.split("/")[-1]
                
                # Construct expected agg_metrics.csv path
                agg_metrics_path = f"s3://{run_dir}/{run_name}__agg_metrics.csv"
                
                # Check if file exists
                if fs.exists(agg_metrics_path.replace("s3://", "")):
                    agg_metrics_files.append(agg_metrics_path)
    
    except Exception as e:
        raise RuntimeError(f"Error listing S3 directories in {batch_path}: {e}")
    
    return agg_metrics_files


def get_run_directory_name(file_path: str) -> str:
    """
    Extract the run directory name from the file path.
    
    Args:
        file_path: Full S3 path to the agg_metrics.csv file
        
    Returns:
        Run directory name
    """
    # Split path and get parent directory name
    path_parts = file_path.rstrip("/").split("/")
    # The run directory is the parent of the CSV file
    return path_parts[-2]


def clean_agg_metrics_data(
    df: pd.DataFrame, 
    run_dir_name: str, 
    logger: logging.Logger
) -> Tuple[pd.DataFrame, dict]:
    """
    Clean the agg_metrics dataframe.
    
    Args:
        df: Original dataframe
        run_dir_name: Name of the run directory
        logger: Logger instance
        
    Returns:
        Tuple of (cleaned dataframe, statistics dictionary)
    """
    stats = {
        "original_rows": len(df),
        "rows_removed_stac_mismatch": 0,
        "rows_removed_duplicates": 0,
        "final_rows": 0
    }
    
    # Step 1: Filter rows where stac_item_id matches run directory
    if "stac_item_id" in df.columns:
        valid_mask = df["stac_item_id"] == run_dir_name
        invalid_rows = (~valid_mask).sum()
        
        if invalid_rows > 0:
            logger.info(f"  Removing {invalid_rows} rows with non-matching stac_item_id")
            df = df[valid_mask].copy()
            stats["rows_removed_stac_mismatch"] = invalid_rows
    else:
        logger.warning("  Column 'stac_item_id' not found in dataframe")
    
    # Step 2: Handle duplicates based on (collection_id, stac_item_id, scenario)
    index_cols = ["collection_id", "stac_item_id", "scenario"]
    
    # Check if all index columns exist
    missing_cols = [col for col in index_cols if col not in df.columns]
    if missing_cols:
        logger.warning(f"  Missing index columns: {missing_cols}")
    else:
        # Find duplicates
        duplicated_mask = df.duplicated(subset=index_cols, keep=False)
        
        if duplicated_mask.any():
            # Process duplicates
            df_no_dups = df[~duplicated_mask].copy()
            df_dups = df[duplicated_mask].copy()
            
            # For duplicates, calculate combined score and keep best
            if "true_positives_count" in df.columns and "false_positives_count" in df.columns:
                df_dups["combined_score"] = (
                    df_dups["true_positives_count"].fillna(0) + 
                    df_dups["false_positives_count"].fillna(0)
                )
                
                # Group by index columns and keep row with max combined score
                idx_to_keep = df_dups.groupby(index_cols)["combined_score"].idxmax()
                df_best = df_dups.loc[idx_to_keep].drop(columns=["combined_score"])
                
                # Combine non-duplicates with best duplicates
                df = pd.concat([df_no_dups, df_best], ignore_index=True)
                
                rows_removed = len(df_dups) - len(df_best)
                if rows_removed > 0:
                    logger.info(f"  Removed {rows_removed} duplicate rows")
                    stats["rows_removed_duplicates"] = rows_removed
            else:
                logger.warning("  Columns for scoring not found, keeping first duplicate")
                df = df.drop_duplicates(subset=index_cols, keep="first")
    
    stats["final_rows"] = len(df)
    return df, stats


def backup_file(file_path: str, fs: fsspec.AbstractFileSystem, logger: logging.Logger) -> bool:
    """
    Create a backup of the original file.
    
    Args:
        file_path: S3 path to the file
        fs: Filesystem instance
        logger: Logger instance
        
    Returns:
        True if backup was successful
    """
    backup_path = file_path.replace(".csv", ".csv.backup")
    s3_path = file_path.replace("s3://", "")
    s3_backup_path = backup_path.replace("s3://", "")
    
    try:
        # Copy file to backup
        fs.copy(s3_path, s3_backup_path)
        logger.info(f"  Created backup: {backup_path}")
        return True
    except Exception as e:
        logger.error(f"  Failed to create backup: {e}")
        return False


def process_agg_metrics_file(
    file_path: str, 
    aws_profile: str,
    logger: logging.Logger,
    dry_run: bool = False
) -> Optional[dict]:
    """
    Process a single agg_metrics.csv file.
    
    Args:
        file_path: S3 path to the agg_metrics.csv file
        aws_profile: AWS profile to use
        logger: Logger instance
        dry_run: If True, don't modify files
        
    Returns:
        Statistics dictionary or None if processing failed
    """
    logger.info(f"Processing: {file_path}")
    
    # Get run directory name
    run_dir_name = get_run_directory_name(file_path)
    logger.info(f"  Run directory: {run_dir_name}")
    
    # Initialize filesystem
    fs = fsspec.filesystem("s3", profile=aws_profile)
    
    try:
        # Read the CSV file
        with fsspec.open(file_path, "r", s3={"profile": aws_profile}) as f:
            df = pd.read_csv(f)
        
        if df.empty:
            logger.warning("  File is empty, skipping")
            return None
        
        df_cleaned, stats = clean_agg_metrics_data(df, run_dir_name, logger)
        
        # Check if any changes were made
        if stats["rows_removed_stac_mismatch"] > 0 or stats["rows_removed_duplicates"] > 0:
            if not dry_run:
                # Create backup
                if not backup_file(file_path, fs, logger):
                    logger.error("  Skipping modification due to backup failure")
                    return None
                
                # Write cleaned data back
                with fsspec.open(file_path, "w", s3={"profile": aws_profile}) as f:
                    df_cleaned.to_csv(f, index=False)
                
                logger.info(f"Modified: {stats['original_rows']} to {stats['final_rows']} rows")
            else:
                logger.info(f"  [DRY RUN] Would modify: {stats['original_rows']} to {stats['final_rows']} rows")
            
            return stats
        else:
            logger.info("  No changes needed")
            return None
            
    except Exception as e:
        logger.error(f"  Error processing file: {e}")
        return None


def main():
    """
    Clean agg_metrics.csv files in S3 batch directories.

    This script:
    1. Finds all agg_metrics.csv files under specified S3 batch directories
    2. Validates that stac_item_id matches the parent run directory name
    3. Handles duplicates based on (collection_id, stac_item_id, scenario) index
    4. Creates backups before modifying files
    5. Logs all modifications
    """

    parser = argparse.ArgumentParser(
        description="Clean agg_metrics.csv files in S3 batch directories"
    )
    
    parser.add_argument(
        "--s3-prefix",
        required=True,
        help="S3 prefix path before batch directories (e.g., s3://fimc-data/autoeval/batches/)"
    )
    
    parser.add_argument(
        "--batch",
        required=True,
        help="Batch directory name (e.g., fim100_huc12_5m_non_calibrated)"
    )
    
    parser.add_argument(
        "--log-file",
        default="agg_metrics_cleaning.log",
        help="Path to log file (default: agg_metrics_cleaning.log)"
    )
    
    parser.add_argument(
        "--profile",
        default="fimbucket",
        help="AWS profile for S3 access (default: fimc-data)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without modifying files"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(args.log_file)
    
    # Log start
    logger.info("=" * 80)
    logger.info(f"Starting agg_metrics cleaning - {datetime.now()}")
    logger.info(f"S3 Prefix: {args.s3_prefix}")
    logger.info(f"Batch: {args.batch}")
    logger.info(f"Dry Run: {args.dry_run}")
    logger.info("=" * 80)
    
    try:
        # Find all agg_metrics.csv files
        logger.info("Searching for agg_metrics.csv files...")
        agg_files = find_agg_metrics_files(args.s3_prefix, args.batch, args.profile)
        
        if not agg_files:
            logger.warning("No agg_metrics.csv files found")
            return 0
        
        logger.info(f"Found {len(agg_files)} agg_metrics.csv files")
        
        # Process each file
        total_stats = {
            "files_processed": 0,
            "files_modified": 0,
            "total_rows_removed": 0
        }
        
        for file_path in agg_files:
            stats = process_agg_metrics_file(
                file_path, 
                args.profile, 
                logger, 
                args.dry_run
            )
            
            if stats:
                total_stats["files_modified"] += 1
                total_stats["total_rows_removed"] += (
                    stats["rows_removed_stac_mismatch"] + 
                    stats["rows_removed_duplicates"]
                )
            
            total_stats["files_processed"] += 1
        
        # Log summary
        logger.info("=" * 80)
        logger.info("SUMMARY")
        logger.info(f"Files processed: {total_stats['files_processed']}")
        logger.info(f"Files modified: {total_stats['files_modified']}")
        logger.info(f"Total rows removed: {total_stats['total_rows_removed']}")
        
        if args.dry_run:
            logger.info("DRY RUN - No files were actually modified")
        
        logger.info(f"Log file: {args.log_file}")
        logger.info("=" * 80)
        
        return 0
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
