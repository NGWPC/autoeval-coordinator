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
        """Initialize the database and create tables if they don't exist."""
        async with self._lock:
            await self._create_tables()
            logger.info(f"Database initialized at {self.db_path}")
    
    async def _create_tables(self) -> None:
        """Create the pipeline_runs and job_status tables if they don't exist."""
        create_pipeline_runs_sql = """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_id TEXT UNIQUE NOT NULL,  -- HUC code (e.g., "12040101")
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            inundation_jobs TEXT,  -- JSON string: {catchment_id: job_id}
            mosaic_jobs TEXT,      -- JSON string: [job_id1, job_id2, ...]
            agreement_jobs TEXT    -- JSON string: [job_id1, job_id2, ...]
        )
        """
        
        create_job_status_sql = """
        CREATE TABLE IF NOT EXISTS job_status (
            job_id TEXT PRIMARY KEY NOT NULL,
            pipeline_id TEXT NOT NULL,
            status TEXT NOT NULL,  -- dispatched, allocated, running, succeeded, failed, etc.
            updated_at TEXT NOT NULL,
            FOREIGN KEY (pipeline_id) REFERENCES pipeline_runs(pipeline_id)
        )
        """
        
        # Create index for efficient pipeline_id lookups
        create_index_sql = """
        CREATE INDEX IF NOT EXISTS idx_job_status_pipeline_id 
        ON job_status(pipeline_id)
        """
        
        create_pipeline_results_sql = """
        CREATE TABLE IF NOT EXISTS pipeline_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_id TEXT NOT NULL,
            scenario_id TEXT NOT NULL,
            collection_name TEXT NOT NULL,
            scenario_name TEXT NOT NULL,
            catchment_count INTEGER NOT NULL,
            total_scenarios INTEGER NOT NULL,
            flowfile_path TEXT,
            mosaic_output TEXT,
            benchmark_mosaic_output TEXT,
            agreement_output TEXT,
            agreement_metrics_path TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (pipeline_id) REFERENCES pipeline_runs(pipeline_id)
        )
        """
        
        # Create index for pipeline_id lookups on results
        create_results_index_sql = """
        CREATE INDEX IF NOT EXISTS idx_pipeline_results_pipeline_id 
        ON pipeline_results(pipeline_id)
        """
        
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(create_pipeline_runs_sql)
            await db.execute(create_job_status_sql)
            await db.execute(create_index_sql)
            await db.execute(create_pipeline_results_sql)
            await db.execute(create_results_index_sql)
            await db.commit()
    
    async def create_pipeline_run(self, pipeline_id: str) -> None:
        """
        Create a new pipeline run record and clean up stale jobs from previous runs.
        
        Args:
            pipeline_id: HUC code serving as unique identifier for the pipeline run
        """
        now = datetime.utcnow().isoformat()
        
        # First, clean up any stale jobs and results from previous runs of this pipeline
        await self._cleanup_stale_jobs(pipeline_id)
        await self._cleanup_stale_results(pipeline_id)
        
        insert_sql = """
        INSERT OR REPLACE INTO pipeline_runs 
        (pipeline_id, created_at, updated_at, inundation_jobs, mosaic_jobs, agreement_jobs)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        
        async with self._lock:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(insert_sql, (
                    pipeline_id, now, now, 
                    json.dumps({}), json.dumps([]), json.dumps([])
                ))
                await db.commit()
        
        logger.info(f"Created pipeline run record: {pipeline_id}")
    
    async def update_inundation_jobs(self, pipeline_id: str, catchment_jobs: Dict[str, str]) -> None:
        """
        Update inundation jobs for a pipeline run.
        
        Args:
            pipeline_id: Pipeline identifier
            catchment_jobs: Dictionary mapping catchment_id to nomad job_id
        """
        await self._update_jobs_field(pipeline_id, "inundation_jobs", catchment_jobs)
        logger.debug(f"Updated inundation jobs for {pipeline_id}: {len(catchment_jobs)} jobs")
    
    async def update_mosaic_jobs(self, pipeline_id: str, job_ids: List[str]) -> None:
        """
        Update mosaic jobs for a pipeline run.
        
        Args:
            pipeline_id: Pipeline identifier
            job_ids: List of nomad job IDs
        """
        await self._update_jobs_field(pipeline_id, "mosaic_jobs", job_ids)
        logger.debug(f"Updated mosaic jobs for {pipeline_id}: {len(job_ids)} jobs")
    
    async def update_agreement_jobs(self, pipeline_id: str, job_ids: List[str]) -> None:
        """
        Update agreement jobs for a pipeline run.
        
        Args:
            pipeline_id: Pipeline identifier
            job_ids: List of nomad job IDs
        """
        await self._update_jobs_field(pipeline_id, "agreement_jobs", job_ids)
        logger.debug(f"Updated agreement jobs for {pipeline_id}: {len(job_ids)} jobs")
    
    async def _update_jobs_field(self, pipeline_id: str, field_name: str, jobs_data: Union[Dict, List]) -> None:
        """
        Update a specific jobs field for a pipeline run.
        
        Args:
            pipeline_id: Pipeline identifier
            field_name: Name of the field to update (inundation_jobs, mosaic_jobs, agreement_jobs)
            jobs_data: Data to store (dict for inundation, list for others)
        """
        now = datetime.utcnow().isoformat()
        jobs_json = json.dumps(jobs_data)
        
        update_sql = f"""
        UPDATE pipeline_runs 
        SET {field_name} = ?, updated_at = ?
        WHERE pipeline_id = ?
        """
        
        async with self._lock:
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute(update_sql, (jobs_json, now, pipeline_id))
                if cursor.rowcount == 0:
                    logger.warning(f"No pipeline run found for ID: {pipeline_id}")
                await db.commit()
    
    async def get_pipeline_run(self, pipeline_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a pipeline run by ID.
        
        Args:
            pipeline_id: Pipeline identifier
            
        Returns:
            Dictionary with pipeline run data or None if not found
        """
        select_sql = """
        SELECT pipeline_id, created_at, updated_at, 
               inundation_jobs, mosaic_jobs, agreement_jobs
        FROM pipeline_runs
        WHERE pipeline_id = ?
        """
        
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(select_sql, (pipeline_id,)) as cursor:
                row = await cursor.fetchone()
                
                if row:
                    return {
                        "pipeline_id": row[0],
                        "created_at": row[1],
                        "updated_at": row[2],
                        "inundation_jobs": json.loads(row[3]) if row[3] else {},
                        "mosaic_jobs": json.loads(row[4]) if row[4] else [],
                        "agreement_jobs": json.loads(row[5]) if row[5] else []
                    }
                return None
    
    async def get_all_pipeline_runs(self) -> List[Dict[str, Any]]:
        """
        Get all pipeline runs.
        
        Returns:
            List of dictionaries with pipeline run data
        """
        select_sql = """
        SELECT pipeline_id, created_at, updated_at, 
               inundation_jobs, mosaic_jobs, agreement_jobs
        FROM pipeline_runs
        ORDER BY created_at DESC
        """
        
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(select_sql) as cursor:
                rows = await cursor.fetchall()
                
                return [
                    {
                        "pipeline_id": row[0],
                        "created_at": row[1],
                        "updated_at": row[2],
                        "inundation_jobs": json.loads(row[3]) if row[3] else {},
                        "mosaic_jobs": json.loads(row[4]) if row[4] else [],
                        "agreement_jobs": json.loads(row[5]) if row[5] else []
                    }
                    for row in rows
                ]
    
    async def delete_pipeline_run(self, pipeline_id: str) -> bool:
        """
        Delete a pipeline run by ID.
        
        Args:
            pipeline_id: Pipeline identifier
            
        Returns:
            True if deleted, False if not found
        """
        delete_sql = "DELETE FROM pipeline_runs WHERE pipeline_id = ?"
        
        async with self._lock:
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute(delete_sql, (pipeline_id,))
                await db.commit()
                return cursor.rowcount > 0
    
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
            logger.info(f"Cleaned up {deleted_count} stale job records for pipeline {pipeline_id}")
    
    async def update_job_status(self, job_id: str, pipeline_id: str, status: str) -> None:
        """
        Update or create a job status record.
        
        Args:
            job_id: Nomad job ID
            pipeline_id: Pipeline identifier (HUC code)
            status: Job status (dispatched, allocated, running, succeeded, failed, etc.)
        """
        now = datetime.utcnow().isoformat()
        
        upsert_sql = """
        INSERT OR REPLACE INTO job_status (job_id, pipeline_id, status, updated_at)
        VALUES (?, ?, ?, ?)
        """
        
        async with self._lock:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(upsert_sql, (job_id, pipeline_id, status, now))
                await db.commit()
    
    async def batch_update_job_status(self, job_updates: List[tuple[str, str, str]]) -> None:
        """
        Batch update multiple job statuses efficiently.
        
        Args:
            job_updates: List of tuples (job_id, pipeline_id, status)
        """
        if not job_updates:
            return
            
        now = datetime.utcnow().isoformat()
        
        upsert_sql = """
        INSERT OR REPLACE INTO job_status (job_id, pipeline_id, status, updated_at)
        VALUES (?, ?, ?, ?)
        """
        
        async with self._lock:
            async with aiosqlite.connect(self.db_path) as db:
                await db.executemany(
                    upsert_sql, 
                    [(job_id, pipeline_id, status, now) for job_id, pipeline_id, status in job_updates]
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
        SELECT job_id, pipeline_id, status, updated_at
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
                        "updated_at": row[3]
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
        SELECT job_id, pipeline_id, status, updated_at
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
                        "updated_at": row[3]
                    }
                    for row in rows
                ]
    
    async def _cleanup_stale_results(self, pipeline_id: str) -> None:
        """
        Clean up stale result records from previous runs of a pipeline.
        
        Args:
            pipeline_id: Pipeline identifier (HUC code)
        """
        delete_sql = "DELETE FROM pipeline_results WHERE pipeline_id = ?"
        
        async with self._lock:
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute(delete_sql, (pipeline_id,))
                deleted_count = cursor.rowcount
                await db.commit()
                
        if deleted_count > 0:
            logger.info(f"Cleaned up {deleted_count} stale result records for pipeline {pipeline_id}")
    
    async def save_pipeline_results(self, result: Dict[str, Any]) -> None:
        """
        Save flattened pipeline results to database.
        
        Args:
            result: Result dictionary containing pipeline_id, scenarios, and counts
        """
        pipeline_id = result.get("pipeline_id", "")
        catchment_count = result.get("catchment_count", 0)
        total_scenarios = result.get("total_scenarios", 0)
        scenarios = result.get("scenarios", [])
        
        if not pipeline_id or not scenarios:
            logger.warning("No pipeline_id or scenarios to save")
            return
        
        now = datetime.utcnow().isoformat()
        
        insert_sql = """
        INSERT INTO pipeline_results (
            pipeline_id, scenario_id, collection_name, scenario_name,
            catchment_count, total_scenarios, flowfile_path,
            mosaic_output, benchmark_mosaic_output, agreement_output,
            agreement_metrics_path, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Prepare batch insert data
        rows = []
        for scenario in scenarios:
            rows.append((
                pipeline_id,
                scenario.get("scenario_id", ""),
                scenario.get("collection_name", ""),
                scenario.get("scenario_name", ""),
                catchment_count,
                total_scenarios,
                scenario.get("flowfile_path", ""),
                scenario.get("mosaic_output", ""),
                scenario.get("benchmark_mosaic_output", ""),
                scenario.get("agreement_output", ""),
                scenario.get("agreement_metrics_path", ""),
                now
            ))
        
        async with self._lock:
            async with aiosqlite.connect(self.db_path) as db:
                await db.executemany(insert_sql, rows)
                await db.commit()
        
        logger.info(f"Saved {len(rows)} result records for pipeline {pipeline_id}")
    
    async def get_pipeline_results(self, pipeline_id: str) -> List[Dict[str, Any]]:
        """
        Get all results for a pipeline.
        
        Args:
            pipeline_id: Pipeline identifier (HUC code)
            
        Returns:
            List of dictionaries with result data
        """
        select_sql = """
        SELECT pipeline_id, scenario_id, collection_name, scenario_name,
               catchment_count, total_scenarios, flowfile_path,
               mosaic_output, benchmark_mosaic_output, agreement_output,
               agreement_metrics_path, created_at
        FROM pipeline_results
        WHERE pipeline_id = ?
        ORDER BY created_at DESC
        """
        
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(select_sql, (pipeline_id,)) as cursor:
                rows = await cursor.fetchall()
                
                return [
                    {
                        "pipeline_id": row[0],
                        "scenario_id": row[1],
                        "collection_name": row[2],
                        "scenario_name": row[3],
                        "catchment_count": row[4],
                        "total_scenarios": row[5],
                        "flowfile_path": row[6],
                        "mosaic_output": row[7],
                        "benchmark_mosaic_output": row[8],
                        "agreement_output": row[9],
                        "agreement_metrics_path": row[10],
                        "created_at": row[11]
                    }
                    for row in rows
                ]
    
    async def close(self) -> None:
        """Close the log database connection."""
        logger.info("Pipeline log database closed")