import json
import os
from typing import Dict, List, Tuple, Optional
import logging
from dataclasses import dataclass, field  # Assuming AppConfig is defined/imported


# --- (Assume AppConfig and other necessary dataclasses are defined or imported) ---
# --- Re-define necessary dataclasses if running standalone ---
@dataclass
class NomadConfig:
    address: str
    token: Optional[str] = None
    namespace: str = "*"


@dataclass
class JobNames:
    hand_inundator: str
    fim_mosaicker: str


@dataclass
class S3Config:
    bucket: str
    base_prefix: str = "pipeline-runs"


@dataclass
class DataPaths:
    mock_catchment_data: str = "mock_catchments.json"
    forecast_csv_template: str = (
        "forecasts/{region}/forecast_{polygon_id}.csv"  # Simplified template
    )


@dataclass
class Defaults:
    geo_mem_cache_inundator: int = 512
    geo_mem_cache_mosaicker: int = 256
    fim_type: str = "extent"
    mosaic_resolution: float = 10.0
    window_size: int = 2


@dataclass
class AppConfig:
    nomad: NomadConfig
    jobs: JobNames
    s3: S3Config
    data_paths: DataPaths
    defaults: Defaults = field(default_factory=Defaults)


# --- End Re-definitions ---


def generate_s3_path(
    config: AppConfig,
    pipeline_id: str,
    job_id: str = None,
    filename: str = None,
    is_input: bool = False,
    catchment_id: str = None,
) -> str:
    """Generates a structured S3 path for pipeline artifacts within the configured bucket."""
    # (Implementation remains the same as before - it correctly uses config.s3.bucket)
    parts = [
        f"s3://{config.s3.bucket}",
        config.s3.base_prefix,
        f"pipeline_{pipeline_id}",
    ]
    if job_id:
        job_suffix = job_id.split("/")[-1] if "/" in job_id else job_id
        parts.append(f"job_{job_suffix}")
    elif catchment_id:
        parts.append(f"catchment_{catchment_id}")
    if is_input:
        parts.append("inputs")
    else:
        parts.append("outputs")
    if filename:
        parts.append(filename)
    return "/".join(parts)


def _stringify_meta(meta: Dict) -> Dict:
    """Ensures all values in the metadata dictionary are strings."""
    return {k: str(v) if v is not None else "" for k, v in meta.items()}


def prepare_inundator_dispatch_info(
    config: AppConfig,
    pipeline_id: str,
    polygon_id: str,
    polygon_data: Dict,
    catchment_id: str,
    catchment_info: Dict,
    hand_version: Optional[str],
    temp_base_dir: str = os.path.join(os.getcwd(), "temp_pipeline_data"),
) -> Tuple[str, Dict, Dict]:
    """
    Prepares info for dispatching a hand_inundator job, assuming all data
    is in the single configured S3 bucket.
    """
    logging.debug(
        f"Preparing inundator params for pipeline={pipeline_id}, catchment={catchment_id}"
    )
    s3_bucket = config.s3.bucket  # Get the single bucket name

    # --- 1. Construct Full Input Raster S3 Paths ---
    rem_raster_rel = catchment_info.get("raster_pair", {}).get("rem_raster_path", "")
    catch_raster_rel = catchment_info.get("raster_pair", {}).get(
        "catchment_raster_path", ""
    )
    # Prepend bucket if path is relative
    rem_raster_s3 = (
        f"s3://{s3_bucket}/{rem_raster_rel}"
        if rem_raster_rel and not rem_raster_rel.startswith("s3://")
        else rem_raster_rel
    )
    catch_raster_s3 = (
        f"s3://{s3_bucket}/{catch_raster_rel}"
        if catch_raster_rel and not catch_raster_rel.startswith("s3://")
        else catch_raster_rel
    )

    # --- 2. Prepare Content for Catchment JSON File ---
    json_content = {
        "catchment_id": catchment_id,
        "hand_version": hand_version,
        "raster_pair": {
            "rem_raster_path": rem_raster_s3,
            "catchment_raster_path": catch_raster_s3,
        },
        "hydrotable_entries": catchment_info.get("hydrotable_entries", {}),
    }

    # --- 3. Determine Local and S3 Paths for Catchment JSON ---
    pipeline_temp_dir = os.path.join(temp_base_dir, pipeline_id)
    os.makedirs(pipeline_temp_dir, exist_ok=True)
    local_json_path_to_create = os.path.join(
        pipeline_temp_dir, f"catchment_{catchment_id}.json"
    )
    # This path is for the generated JSON *input* to the job
    input_json_s3_path = generate_s3_path(
        config,
        pipeline_id,
        catchment_id=catchment_id,
        filename="catchment_data.json",
        is_input=True,
    )

    # --- 4. Determine Job Output Path ---
    # This path is where the job *writes* its output
    output_fim_s3_path = generate_s3_path(
        config,
        pipeline_id,
        catchment_id=catchment_id,
        filename="inundation_output.tif",
        is_input=False,
    )

    # --- 5. Get Forecast Path ---
    forecast_path_s3 = None
    # Prioritize path directly from polygon_data
    if "forecast_path" in polygon_data:
        fc_path_rel = polygon_data["forecast_path"]
        # Prepend bucket if relative path given in polygon_data
        forecast_path_s3 = (
            f"s3://{s3_bucket}/{fc_path_rel}"
            if fc_path_rel and not fc_path_rel.startswith("s3://")
            else fc_path_rel
        )
    else:
        # Construct from template (which is now relative)
        region = polygon_data.get(
            "region", f"region_{polygon_id}"
        )  # Need a region fallback
        fc_template_rel = (
            config.data_paths.forecast_csv_template
        )  # Relative template path
        try:
            # Format the relative path template
            relative_path = fc_template_rel.format(region=region, polygon_id=polygon_id)
            # Construct the full S3 path
            forecast_path_s3 = f"s3://{s3_bucket}/{relative_path}"
            logging.debug(f"Constructed forecast path: {forecast_path_s3}")
        except KeyError as e:
            # Fallback if template formatting fails
            default_fc_rel_path = "forecasts/unknown/default.csv"
            forecast_path_s3 = f"s3://{s3_bucket}/{default_fc_rel_path}"
            logging.warning(
                f"Missing key '{e}' for forecast path template. Using default: {forecast_path_s3}"
            )
        except Exception as e:
            default_fc_rel_path = "forecasts/unknown/default.csv"
            forecast_path_s3 = f"s3://{s3_bucket}/{default_fc_rel_path}"
            logging.warning(
                f"Error formatting forecast template '{fc_template_rel}': {e}. Using default: {forecast_path_s3}"
            )

    # --- 6. Assemble Dispatch Metadata ---
    dispatch_meta = {
        "pipeline_id": pipeline_id,
        "polygon_id": polygon_id,
        "catchment_id": catchment_id,
        "job_type": "inundator",
        "expected_output_path": output_fim_s3_path,
        "catchment_data_path": input_json_s3_path,
        "forecast_path": forecast_path_s3,  # Use the constructed full S3 path
        "output_path": output_fim_s3_path,
        "window_size": polygon_data.get("window_size", config.defaults.window_size),
        "fim_type": polygon_data.get("fim_type", config.defaults.fim_type),
        "geo_mem_cache": polygon_data.get(
            "geo_mem_cache_inundator", config.defaults.geo_mem_cache_inundator
        ),
    }

    return local_json_path_to_create, json_content, _stringify_meta(dispatch_meta)


def prepare_mosaicker_dispatch_meta(
    config: AppConfig,
    pipeline_id: str,
    polygon_id: str,
    polygon_data: Dict,
    completed_inundator_outputs: List[str],  # These should already be full S3 paths
) -> Dict:
    """Prepares metadata for the fim_mosaicker job dispatch."""
    logging.debug(f"Preparing mosaicker params for pipeline={pipeline_id}")

    # --- 1. Determine Final Output Path ---
    # generate_s3_path correctly uses config.s3.bucket
    final_output_s3_path = generate_s3_path(
        config, pipeline_id, filename="final_mosaic_output.tif", is_input=False
    )

    # --- 2. Assemble Dispatch Metadata ---
    dispatch_meta = {
        "pipeline_id": pipeline_id,
        "polygon_id": polygon_id,
        "job_type": "mosaicker",
        "expected_output_path": final_output_s3_path,
        "raster_paths": json.dumps(
            completed_inundator_outputs
        ),  # Job receives JSON string of S3 paths
        "mosaic_output_path": final_output_s3_path,  # Job writes here
        "resolution": polygon_data.get(
            "mosaic_resolution", config.defaults.mosaic_resolution
        ),
        "fim_type": polygon_data.get("fim_type", config.defaults.fim_type),
        "geo_mem_cache": polygon_data.get(
            "geo_mem_cache_mosaicker", config.defaults.geo_mem_cache_mosaicker
        ),
    }

    return _stringify_meta(dispatch_meta)
