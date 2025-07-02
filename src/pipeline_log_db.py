import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import aiosqlite

logger = logging.getLogger(__name__)


class PipelineLogDB:
    """
    Async SQLite log database for pipeline execution tracking.
    
    Designed to handle:
    - Non-blocking writes to avoid blocking the event loop
    - Multiple pipeline instances writing to the same database
    - Atomic operations with proper locking
    - Complete audit trail of pipeline runs, job statuses, and results
    """

    def __init__(self, db_path: str = "pipeline_log.db"):
        self.db_path = Path(db_path)
        self._lock = asyncio.Lock()
    
    async def initialize(self) -> None:
        async with self._lock:
            await self._create_tables()
            logger.debug(f"Database initialized at {self.db_path}")
    
    async def _create_tables(self) -> None:
        """Create the job_status table if it doesn't exist."""
        create_job_status_sql = """
        CREATE TABLE IF NOT EXISTS job_status (
            job_id TEXT PRIMARY KEY NOT NULL,
            pipeline_id TEXT NOT NULL,
            status TEXT NOT NULL,  -- dispatched, allocated, running, succeeded, failed, etc.
            stage TEXT NOT NULL,   -- inundate, mosaic, or agreement
            write_path TEXT,       -- JSON string: list of write paths that the job wrote to (empty if none)
            updated_at TEXT NOT NULL
        )
        """
        
        # Create index for efficient pipeline_id lookups
        create_index_sql = """
        CREATE INDEX IF NOT EXISTS idx_job_status_pipeline_id 
        ON job_status(pipeline_id)
        """
        
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(create_job_status_sql)
            await db.execute(create_index_sql)
            await db.commit()
    
    async def cleanup_pipeline_jobs(self, pipeline_id: str) -> None:
        """
        Clean up stale job status records from previous runs of a pipeline.
        
        Args:
            pipeline_id: Pipeline identifier (HUC code)
        """
        await self._cleanup_stale_jobs(pipeline_id)
        logger.debug(f"Cleaned up stale jobs for pipeline {pipeline_id}")
    
    
    
    async def _cleanup_stale_jobs(self, pipeline_id: str) -> None:
        """
        Clean up stale job status records from previous runs of a pipeline.
        
        Args:
            pipeline_id: Pipeline identifier (HUC code)
        """
        delete_sql = "DELETE FROM job_status WHERE pipeline_id = ?"
        
        async with self._lock:
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute(delete_sql, (pipeline_id,))
                deleted_count = cursor.rowcount
                await db.commit()
                
        if deleted_count > 0:
            logger.debug(f"Cleaned up {deleted_count} stale job records for pipeline {pipeline_id}")
    
    async def update_job_status(self, job_id: str, pipeline_id: str, status: str, stage: str, write_paths: Optional[List[str]] = None) -> None:
        """
        Update or create a job status record.
        
        Args:
            job_id: Nomad job ID
            pipeline_id: Pipeline identifier (HUC code)
            status: Job status (dispatched, allocated, running, succeeded, failed, etc.)
            stage: Pipeline stage (inundate, mosaic, or agreement)
            write_paths: List of write paths that the job wrote to (empty if none)
        """
        now = datetime.utcnow().isoformat()
        write_paths_json = json.dumps(write_paths or [])
        
        upsert_sql = """
        INSERT OR REPLACE INTO job_status (job_id, pipeline_id, status, stage, write_path, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        
        async with self._lock:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(upsert_sql, (job_id, pipeline_id, status, stage, write_paths_json, now))
                await db.commit()
    
    async def batch_update_job_status(self, job_updates: List[tuple[str, str, str, str, Optional[List[str]]]]) -> None:
        """
        Batch update multiple job statuses efficiently.
        
        Args:
            job_updates: List of tuples (job_id, pipeline_id, status, stage, write_paths)
        """
        if not job_updates:
            return
            
        now = datetime.utcnow().isoformat()
        
        upsert_sql = """
        INSERT OR REPLACE INTO job_status (job_id, pipeline_id, status, stage, write_path, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        
        async with self._lock:
            async with aiosqlite.connect(self.db_path) as db:
                await db.executemany(
                    upsert_sql, 
                    [(job_id, pipeline_id, status, stage, json.dumps(write_paths or []), now) 
                     for job_id, pipeline_id, status, stage, write_paths in job_updates]
                )
                await db.commit()
        
        logger.debug(f"Batch updated {len(job_updates)} job statuses")
    
    async def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get status for a specific job.
        
        Args:
            job_id: Nomad job ID
            
        Returns:
            Dictionary with job status data or None if not found
        """
        select_sql = """
        SELECT job_id, pipeline_id, status, stage, write_path, updated_at
        FROM job_status
        WHERE job_id = ?
        """
        
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(select_sql, (job_id,)) as cursor:
                row = await cursor.fetchone()
                
                if row:
                    return {
                        "job_id": row[0],
                        "pipeline_id": row[1],
                        "status": row[2],
                        "stage": row[3],
                        "write_paths": json.loads(row[4]) if row[4] else [],
                        "updated_at": row[5]
                    }
                return None
    
    async def get_pipeline_job_statuses(self, pipeline_id: str) -> List[Dict[str, Any]]:
        """
        Get all job statuses for a pipeline.
        
        Args:
            pipeline_id: Pipeline identifier (HUC code)
            
        Returns:
            List of dictionaries with job status data
        """
        select_sql = """
        SELECT job_id, pipeline_id, status, stage, write_path, updated_at
        FROM job_status
        WHERE pipeline_id = ?
        ORDER BY updated_at DESC
        """
        
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(select_sql, (pipeline_id,)) as cursor:
                rows = await cursor.fetchall()
                
                return [
                    {
                        "job_id": row[0],
                        "pipeline_id": row[1],
                        "status": row[2],
                        "stage": row[3],
                        "write_paths": json.loads(row[4]) if row[4] else [],
                        "updated_at": row[5]
                    }
                    for row in rows
                ]
    
    
    async def close(self) -> None:
        """Close the log database connection."""
        logger.debug("Pipeline log database closed")