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

    def __init__(self, config: AppConfig, outputs_path: str, aoi_name: str):
        self.config = config
        self.aoi_name = aoi_name

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
        """Generate path: base/SOURCE/SCENARIO/catchment-extents/filename"""
        return f"{self.base}/{collection_name}/{scenario_name}/catchment-extents/{filename}"

    def inundation_output_path(self, collection_name: str, scenario_name: str, catchment_id: str) -> str:
        filename = f"{collection_name}__{scenario_name}__catchment-{catchment_id}.tif"
        return self.catchment_path(collection_name, scenario_name, catchment_id, filename)

    def catchment_data_indices_dir(self) -> str:
        """Generate path for catchment data indices directory: base/catchment-data-indices/"""
        return f"{self.base}/catchment-data-indices"

    def catchment_parquet_path(self, catchment_id: str) -> str:
        """Generate path for catchment parquet file: base/catchment-data-indices/catchment-{id}.parquet"""
        return f"{self.catchment_data_indices_dir()}/catchment-{catchment_id}.parquet"

    def flowfile_path(self, collection_name: str, scenario_name: str) -> str:
        filename = f"{self.aoi_name}__{collection_name}__{scenario_name}__flowfile.csv"
        return self.source_scenario_path(collection_name, scenario_name, filename)

    def hand_mosaic_path(self, collection_name: str, scenario_name: str) -> str:
        filename = f"{self.aoi_name}__{collection_name}__{scenario_name}__inundate_mosaic.tif"
        return self.source_scenario_path(collection_name, scenario_name, filename)

    def benchmark_mosaic_path(self, collection_name: str, scenario_name: str) -> str:
        filename = f"{self.aoi_name}__{collection_name}__{scenario_name}__benchmark_mosaic.tif"
        return self.source_scenario_path(collection_name, scenario_name, filename)

    def agreement_map_path(self, collection_name: str, scenario_name: str) -> str:
        filename = f"{self.aoi_name}__{collection_name}__{scenario_name}__agreement.tif"
        return self.source_scenario_path(collection_name, scenario_name, filename)

    def agreement_metrics_path(self, collection_name: str, scenario_name: str) -> str:
        filename = f"{self.aoi_name}__{collection_name}__{scenario_name}__metrics.csv"
        return self.source_scenario_path(collection_name, scenario_name, filename)

    def logs_path(self) -> str:
        """Generate path for pipeline logs: base/{aoi}__logs.txt"""
        filename = f"{self.aoi_name}__logs.txt"
        return f"{self.base}/{filename}"

    def results_path(self) -> str:
        """Generate path for aggregated results: base/{aoi}__agg_metrics.csv"""
        filename = f"{self.aoi_name}__agg_metrics.csv"
        return f"{self.base}/{filename}"

    def results_json_path(self) -> str:
        """Generate path for pipeline results JSON: base/{aoi}__results.json"""
        filename = f"{self.aoi_name}__results.json"
        return f"{self.base}/{filename}"

    def aoi_path(self) -> str:
        """Generate path for AOI file: base/{aoi}__aoi.gpkg"""
        filename = f"{self.aoi_name}__aoi.gpkg"
        return f"{self.base}/{filename}"
