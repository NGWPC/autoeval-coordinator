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

    def __init__(self, config: AppConfig, pipeline_id: str, outputs_path: str):
        self.config = config
        self.pipeline_id = pipeline_id
        
        # Use provided outputs_path (local or S3)
        if outputs_path.startswith("s3://"):
            self.base = f"{outputs_path.rstrip('/')}/pipeline_{pipeline_id}"
        else:
            # Local path
            self.base = str(Path(outputs_path) / f"pipeline_{pipeline_id}")
            Path(self.base).mkdir(parents=True, exist_ok=True)

    def scenario_path(self, scenario_id: str, filename: str) -> str:
        return f"{self.base}/scenario_{scenario_id}/{filename}"

    def catchment_path(self, scenario_id: str, catchment_id: str, filename: str) -> str:
        return f"{self.base}/scenario_{scenario_id}/catchment_{catchment_id}/{filename}"

    def inundation_output_path(self, scenario_id: str, catchment_id: str) -> str:
        return self.catchment_path(scenario_id, catchment_id, "inundation_output.tif")

    def hand_mosaic_path(self, scenario_id: str) -> str:
        return self.scenario_path(scenario_id, "HAND_mosaic.tif")

    def benchmark_mosaic_path(self, scenario_id: str) -> str:
        return self.scenario_path(scenario_id, "benchmark_mosaic.tif")

    def agreement_map_path(self, scenario_id: str) -> str:
        return self.scenario_path(scenario_id, "agreement_map.tif")

    def agreement_metrics_path(self, scenario_id: str) -> str:
        return self.scenario_path(scenario_id, "agreement_metrics.csv")
