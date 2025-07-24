from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from load_config import AppConfig


@dataclass
class PipelineResult:
    """Unified result object that tracks pipeline state and file paths."""

    scenario_id: str
    collection_name: str
    scenario_name: str
    flowfile_path: str
    benchmark_rasters: List[str] = field(default_factory=list)
    status: str = "pending"
    paths: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None

    def get_path(self, stage: str, file_type: str) -> Optional[str]:
        return self.paths.get(f"{stage}_{file_type}")

    def set_path(self, stage: str, file_type: str, path: str):
        self.paths[f"{stage}_{file_type}"] = path

    def mark_failed(self, error: str):
        self.status = "failed"
        self.error = error

    def mark_completed(self):
        self.status = "completed"


class PathFactory:
    """Centralized path generation for pipeline stages, supporting both local and S3 paths."""

    def __init__(self, config: AppConfig, outputs_path: str):
        self.config = config

        # Use provided outputs_path (local or S3) - this should already include the HUC directory
        if outputs_path.startswith("s3://"):
            self.base = outputs_path.rstrip("/")
        else:
            # Local path
            self.base = str(Path(outputs_path))
            Path(self.base).mkdir(parents=True, exist_ok=True)

    def source_scenario_path(self, collection_name: str, scenario_name: str, filename: str) -> str:
        """Generate path: base/SOURCE/SCENARIO/filename"""
        return f"{self.base}/{collection_name}/{scenario_name}/{filename}"

    def catchment_path(self, collection_name: str, scenario_name: str, catchment_id: str, filename: str) -> str:
        """Generate path: base/SOURCE/SCENARIO/catchments/catchment-ID/filename"""
        return f"{self.base}/{collection_name}/{scenario_name}/catchments/catchment-{catchment_id}/{filename}"

    def inundation_output_path(self, collection_name: str, scenario_name: str, catchment_id: str) -> str:
        return self.catchment_path(collection_name, scenario_name, catchment_id, f"catchment-{catchment_id}.tif")

    def inundation_parquet_path(self, collection_name: str, scenario_name: str, catchment_id: str) -> str:
        return self.catchment_path(collection_name, scenario_name, catchment_id, f"catchment-{catchment_id}.parquet")

    def flowfile_path(self, collection_name: str, scenario_name: str) -> str:
        return self.source_scenario_path(collection_name, scenario_name, "flowfile.csv")

    def hand_mosaic_path(self, collection_name: str, scenario_name: str) -> str:
        return self.source_scenario_path(collection_name, scenario_name, "inundate_mosaic.tif")

    def benchmark_mosaic_path(self, collection_name: str, scenario_name: str) -> str:
        return self.source_scenario_path(collection_name, scenario_name, "benchmark_mosaic.tif")

    def agreement_map_path(self, collection_name: str, scenario_name: str) -> str:
        return self.source_scenario_path(collection_name, scenario_name, "agreement.tif")

    def agreement_metrics_path(self, collection_name: str, scenario_name: str) -> str:
        return self.source_scenario_path(collection_name, scenario_name, "metrics.csv")

    def logs_path(self) -> str:
        """Generate path for pipeline logs: base/logs.txt"""
        return f"{self.base}/logs.txt"

    def results_path(self) -> str:
        """Generate path for aggregated results: base/agg_metrics.csv"""
        return f"{self.base}/agg_metrics.csv"

    def results_json_path(self) -> str:
        """Generate path for pipeline results JSON: base/results.json"""
        return f"{self.base}/results.json"

    def aoi_path(self) -> str:
        """Generate path for AOI file: base/aoi.gpkg"""
        return f"{self.base}/aoi.gpkg"
