import logging
import os
from typing import Any, Dict, List, Literal, Optional

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
    agreement_maker: str = Field(..., examples=["agreement-maker-processor"])


class S3Config(BaseModel):
    bucket: str = Field(..., min_length=3, examples=["your-fim-data-bucket"])
    base_prefix: str = "pipeline-runs"
    # AWS credentials - loaded from .env file
    AWS_ACCESS_KEY_ID: Optional[str] = Field(default_factory=lambda: os.getenv("AWS_ACCESS_KEY_ID"))
    AWS_SECRET_ACCESS_KEY: Optional[str] = Field(default_factory=lambda: os.getenv("AWS_SECRET_ACCESS_KEY"))
    AWS_SESSION_TOKEN: Optional[str] = Field(default_factory=lambda: os.getenv("AWS_SESSION_TOKEN"))
    # Optional: Add additional S3 options if needed
    # e.g., region_name, endpoint_url for specific AWS config
    s3_options: Optional[Dict[str, Any]] = None


class MockDataPaths(BaseModel):
    mock_catchment_data: str = "mock_catchments.json"
    polygon_data_file: str = Field(..., description="Path to polygon GeoDataFrame file (gpkg format)")
    mock_stac_results: Optional[str] = Field(None, description="Path to mock STAC query results JSON")
    huc: Optional[str] = Field(None, description="HUC code for mock polygon data")


class HandIndexConfig(BaseModel):
    partitioned_base_path: str = Field(..., description="Base path to partitioned parquet files (local or s3://)")
    overlap_threshold_percent: float = Field(
        10.0, ge=0.0, le=100.0, description="Minimum overlap percentage to keep a catchment"
    )
    enabled: bool = Field(True, description="Whether to use real hand index queries (True) or mock data (False)")


class StacConfig(BaseModel):
    api_url: str = Field(..., description="STAC API root URL")
    collections: List[str] = Field(..., description="List of STAC collection IDs to query")
    overlap_threshold_percent: float = Field(
        40.0, ge=0.0, le=100.0, description="Minimum overlap percentage to keep a STAC item"
    )
    datetime_filter: Optional[str] = Field(None, description="STAC datetime or interval filter")
    enabled: bool = Field(True, description="Whether to use STAC queries for flow scenarios")


class FlowScenarioConfig(BaseModel):
    output_dir: str = Field("combined_flowfiles", description="Directory to save combined flowfiles")


class WbdConfig(BaseModel):
    gpkg_path: str = Field(..., description="Path to WBD_National.gpkg file")
    huc_list_path: str = Field(..., description="Path to huc_list.txt file")


class Defaults(BaseModel):
    fim_type: Literal["extent", "depth"] = "extent"
    http_connection_limit: int = Field(100, gt=0, description="Max concurrent outgoing HTTP connections")


class AppConfig(BaseModel):
    nomad: NomadConfig
    jobs: JobNames
    s3: S3Config
    mock_data_paths: MockDataPaths
    hand_index: HandIndexConfig
    stac: Optional[StacConfig] = Field(None, description="STAC API configuration")
    flow_scenarios: Optional[FlowScenarioConfig] = Field(None, description="Flow scenario processing configuration")
    wbd: WbdConfig = Field(..., description="WBD National data configuration")
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
