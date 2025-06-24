import logging
import os
from typing import Any, Dict, Literal, Optional

import yaml
from dotenv import load_dotenv
from pydantic import (
    AnyHttpUrl,
    BaseModel,
    Field,
    HttpUrl,
    ValidationError,
    field_validator,
)

# --- Configuration Models ---


class NomadConfig(BaseModel):
    address: AnyHttpUrl = Field(..., examples=["http://127.0.0.1:4646"])
    token: Optional[str] = None
    namespace: str = "*"
    registry_token: Optional[str] = Field(None, description="Token for Docker private registry authentication")


class JobNames(BaseModel):
    hand_inundator: str = Field(..., examples=["hand-inundation-processor"])
    fim_mosaicker: str = Field(..., examples=["fim-mosaic-processor"])


class S3Config(BaseModel):
    bucket: str = Field(..., min_length=3, examples=["your-fim-data-bucket"])
    base_prefix: str = "pipeline-runs"
    # AWS credentials - loaded from .env file
    AWS_ACCESS_KEY_ID: Optional[str] = Field(default_factory=lambda: os.getenv("AWS_ACCESS_KEY_ID"))
    AWS_SECRET_ACCESS_KEY: Optional[str] = Field(default_factory=lambda: os.getenv("AWS_SECRET_ACCESS_KEY"))
    AWS_SESSION_TOKEN: Optional[str] = Field(default_factory=lambda: os.getenv("AWS_SESSION_TOKEN"))
    # Optional: Add transport_params for smart_open/aiobotocore if needed
    # e.g., region_name, profile_name for specific AWS config
    transport_params: Optional[Dict[str, Any]] = None


class MockDataPaths(BaseModel):
    mock_catchment_data: str = "mock_catchments.json"
    polygon_data_file: str = "mock_polygons.json"  # Path to polygon data
    forecast_csv: str = "s3path/to/flowfile.csv"


class HandIndexConfig(BaseModel):
    partitioned_base_path: str = Field(..., description="Base path to partitioned parquet files (local or s3://)")
    overlap_threshold_percent: float = Field(10.0, ge=0.0, le=100.0, description="Minimum overlap percentage to keep a catchment")
    enabled: bool = Field(True, description="Whether to use real hand index queries (True) or mock data (False)")


class Defaults(BaseModel):
    gdal_cache_max: int = Field(512, gt=0, description="GDAL cache size in MB for all jobs")
    fim_type: Literal["extent", "depth"] = "extent"
    # Removed geo_mem_cache_inundator, geo_mem_cache_mosaicker, and mosaic_resolution
    http_connection_limit: int = Field(10, gt=0, description="Max concurrent outgoing HTTP connections")


class AppConfig(BaseModel):
    nomad: NomadConfig
    jobs: JobNames
    s3: S3Config
    mock_data_paths: MockDataPaths
    hand_index: HandIndexConfig
    defaults: Defaults = Field(default_factory=Defaults)


# --- Loading Function ---


def load_config(path: str = "config.yaml") -> AppConfig:
    """
    Loads, parses, and validates the application configuration from a YAML file.
    Also loads environment variables from .env file for AWS credentials.

    Args:
        path: The path to the configuration YAML file.

    Returns:
        An validated AppConfig object.

    Raises:
        FileNotFoundError: If the config file doesn't exist.
        yaml.YAMLError: If the YAML file is malformed.
        ValueError: If the config file is empty or top-level keys are missing.
        ValidationError: If the configuration data fails Pydantic validation.
        Exception: For other unexpected errors during loading.
    """
    load_dotenv(override=True)

    try:
        with open(path, "r") as f:
            raw_config = yaml.safe_load(f)
        if not raw_config:
            raise ValueError(f"Configuration file is empty or invalid: {path}")

        # Use model_validate for Pydantic v2
        config = AppConfig.model_validate(raw_config)
        logging.info(f"Configuration loaded and validated successfully from {path}")
        return config
    except FileNotFoundError:
        logging.error(f"Config file not found: {path}")
        raise
    except yaml.YAMLError as e:
        logging.error(f"Error parsing YAML config file {path}: {e}")
        raise ValueError(f"Invalid YAML format in {path}") from e
    except ValidationError as e:
        # Log the detailed validation errors
        error_details = e.errors()
        logging.error(f"Configuration validation failed for {path}:")
        for error in error_details:
            loc = " -> ".join(map(str, error["loc"]))
            logging.error(f"  - Field: '{loc}' - {error['msg']} (value: {error.get('input')})")
        raise  # Re-raise the validation error
    except Exception as e:
        logging.error(f"Unexpected error loading config from {path}: {e}")
        raise
