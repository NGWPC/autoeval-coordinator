import json
import os
from typing import Dict, List, Tuple, Optional
import logging
from dataclasses import dataclass, field


def generate_s3_path(
    config: AppConfig,
    pipeline_id: str,
    job_id: str = None,
    filename: str = None,
    is_input: bool = False,
    catchment_id: str = None,
) -> str:
    """
    Generates a structured S3 path for pipeline artifacts (outputs or generated inputs)
    within the configured bucket.
    """
    parts = [
        f"s3://{config.s3.bucket}",
        config.s3.base_prefix,
        f"pipeline_{pipeline_id}",
    ]
    # Use specific identifiers for organization
    if job_id:
        job_suffix = job_id.split("/")[-1] if "/" in job_id else job_id
        parts.append(f"job_{job_suffix}")
    elif catchment_id:
        # Group intermediate files by catchment under the pipeline
        parts.append(f"catchment_{catchment_id}")

    # Distinguish inputs generated *by* the pipeline from job outputs
    if is_input:
        parts.append("inputs")  # e.g., the catchment_data.json generated here
    else:
        parts.append("outputs")  # e.g., the inundation .tif file produced by the job

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
    catchment_id: str,
    catchment_info: Dict,
    hand_version: Optional[str],
    temp_base_dir: str = os.path.join(os.getcwd(), "temp_pipeline_data"),
) -> Tuple[str, Dict, Dict]:
    """
    Prepares info for dispatching a hand_inundator job, assuming input data paths
    (rasters, forecast) are provided as full S3 URIs.

    Args:
        config: Application configuration.
        pipeline_id: ID of the current pipeline run.
        polygon_id: ID of the polygon being processed.
        polygon_data: Dictionary containing data specific to the polygon, MUST
                      include 'forecast_path' as a full S3 URI.
        catchment_id: ID of the specific catchment.
        catchment_info: Dictionary containing data for the catchment, MUST include
                        'raster_pair' with 'rem_raster_path' and
                        'catchment_raster_path' as full S3 URIs.
        hand_version: Version identifier for the HAND data used.
        temp_base_dir: Local base directory for temporary files (like the JSON before upload).

    Returns:
        Tuple containing:
        - local_json_path_to_create: The local path where the intermediate JSON should be written.
        - json_content: The dictionary content for the intermediate JSON file.
        - dispatch_meta: Stringified metadata dictionary for the Nomad job dispatch.

    Raises:
        KeyError: If required paths ('forecast_path', 'rem_raster_path',
                  'catchment_raster_path') are missing in input dicts.
        ValueError: If provided paths do not appear to be valid S3 URIs.
    """
    logging.debug(
        f"Preparing inundator params for pipeline={pipeline_id}, catchment={catchment_id}"
    )

    # --- 1. Get Full Input Raster S3 Paths (Assume they are provided) ---
    try:
        raster_pair = catchment_info["raster_pair"]
        rem_raster_s3 = raster_pair["rem_raster_path"]
        catch_raster_s3 = raster_pair["catchment_raster_path"]

        if not rem_raster_s3 or not rem_raster_s3.startswith("s3://"):
            raise ValueError(f"Invalid or missing rem_raster_path: {rem_raster_s3}")
        if not catch_raster_s3 or not catch_raster_s3.startswith("s3://"):
            raise ValueError(
                f"Invalid or missing catchment_raster_path: {catch_raster_s3}"
            )

    except KeyError as e:
        logging.error(
            f"Missing raster path key in catchment_info for {catchment_id}: {e}"
        )
        raise KeyError(f"Missing raster path key in catchment_info: {e}") from e

    # --- 2. Get Full Forecast Path S3 Path (Assume it's provided) ---
    try:
        forecast_path_s3 = polygon_data["forecast_path"]
        if not forecast_path_s3 or not forecast_path_s3.startswith("s3://"):
            raise ValueError(
                f"Invalid or missing forecast_path in polygon_data: {forecast_path_s3}"
            )
    except KeyError:
        logging.error(f"Missing 'forecast_path' key in polygon_data for {polygon_id}")
        raise KeyError(
            "'forecast_path' must be provided in polygon_data as a full S3 URI"
        )

    # --- 3. Prepare Content for Catchment JSON File ---
    # Content uses the validated full S3 paths
    json_content = {
        "catchment_id": catchment_id,
        "hand_version": hand_version,
        "raster_pair": {
            "rem_raster_path": rem_raster_s3,
            "catchment_raster_path": catch_raster_s3,
        },
        "hydrotable_entries": catchment_info.get("hydrotable_entries", {}),
    }

    # --- 4. Determine Local Path for Temp JSON & S3 Path for Uploaded JSON Input ---
    pipeline_temp_dir = os.path.join(temp_base_dir, pipeline_id)
    os.makedirs(pipeline_temp_dir, exist_ok=True)
    local_json_path_to_create = os.path.join(
        pipeline_temp_dir, f"catchment_{catchment_id}_input.json"  # More specific name
    )
    # This S3 path is for the generated JSON *input* to the job
    input_json_s3_path = generate_s3_path(
        config,
        pipeline_id,
        catchment_id=catchment_id,  # Group by catchment
        filename="catchment_data.json",
        is_input=True,
    )

    # --- 5. Determine Job Output Path ---
    # This S3 path is where the job *writes* its output
    output_fim_s3_path = generate_s3_path(
        config,
        pipeline_id,
        catchment_id=catchment_id,  # Group by catchment
        filename="inundation_output.tif",
        is_input=False,  # This is an output of the job
    )

    # --- 6. Assemble Dispatch Metadata ---
    dispatch_meta = {
        "pipeline_id": pipeline_id,
        "polygon_id": polygon_id,
        "catchment_id": catchment_id,
        "job_type": "inundator",
        "catchment_data_path": input_json_s3_path,  # Path to the JSON we generate in the pipeline temp directory
        "forecast_path": f"s3://{config.s3.bucket}/{config.forecast_csv}",  # Path to the forecast CSV on s3 
        "output_path": output_fim_s3_path,  
        "fim_type": config.defaults.fim_type,
        "geo_mem_cache":  config.defaults.geo_mem_cache_inundator
        ),
    }

    return local_json_path_to_create, json_content, _stringify_meta(dispatch_meta)


def prepare_mosaicker_dispatch_meta(
    config: AppConfig,
    pipeline_id: str,
    polygon_id: str,
    completed_inundator_outputs: List[
        str
    ],  
) -> Dict:
    """
    Prepares metadata for the fim_mosaicker job dispatch. Assumes inundator outputs
    are full S3 paths.
    """
    logging.debug(f"Preparing mosaicker params for pipeline={pipeline_id}")

    final_output_s3_path = generate_s3_path(
        config,
        pipeline_id,
        # TODO: add job_id and catchment_id here
        filename=f"final_mosaic_{polygon_id}.tif",
        is_input=False,
    )

    # --- 2. Assemble Dispatch Metadata ---
    dispatch_meta = {
        "pipeline_id": pipeline_id,
        "polygon_id": polygon_id,
        "job_type": "mosaicker",
        "raster_paths": json.dumps(
            completed_inundator_outputs
        ),  # Job receives JSON string of full S3 input paths
        "output_path": final_output_s3_path,  # Job writes final mosaic here
        "fim_type": config.defaults.fim_type,
        "geo_mem_cache": config.defaults.geo_mem_cache_mosaicker,
    }

    return _stringify_meta(dispatch_meta)
