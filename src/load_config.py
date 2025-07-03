import logging
import os
from typing import Any, Dict, List, Literal, Optional

from dotenv import load_dotenv
from pydantic import (
    AnyHttpUrl,
    BaseModel,
    Field,
    HttpUrl,
    ValidationError,
    field_validator,
)

from . import default_config


class NomadConfig(BaseModel):
    address: AnyHttpUrl = Field(
        default_factory=lambda: os.getenv("NOMAD_ADDRESS", default_config.NOMAD_ADDRESS),
        examples=["http://127.0.0.1:4646"],
    )
    token: Optional[str] = Field(default_factory=lambda: os.getenv("NOMAD_TOKEN", default_config.NOMAD_TOKEN) or None)
    namespace: str = Field(default_factory=lambda: os.getenv("NOMAD_NAMESPACE", default_config.NOMAD_NAMESPACE))
    registry_token: Optional[str] = Field(
        default_factory=lambda: os.getenv("NOMAD_REGISTRY_TOKEN", default_config.NOMAD_REGISTRY_TOKEN) or None,
        description="Token for Docker private registry authentication",
    )


class JobNames(BaseModel):
    hand_inundator: str = Field(
        default_factory=lambda: os.getenv("HAND_INUNDATOR_JOB_NAME", default_config.HAND_INUNDATOR_JOB_NAME),
        examples=["hand-inundation-processor"],
    )
    fim_mosaicker: str = Field(
        default_factory=lambda: os.getenv("FIM_MOSAICKER_JOB_NAME", default_config.FIM_MOSAICKER_JOB_NAME),
        examples=["fim-mosaic-processor"],
    )
    agreement_maker: str = Field(
        default_factory=lambda: os.getenv("AGREEMENT_MAKER_JOB_NAME", default_config.AGREEMENT_MAKER_JOB_NAME),
        examples=["agreement-maker-processor"],
    )


class S3Config(BaseModel):
    bucket: str = Field(
        default_factory=lambda: os.getenv("S3_BUCKET", default_config.S3_BUCKET),
        min_length=3,
        examples=["your-fim-data-bucket"],
    )
    base_prefix: str = Field(default_factory=lambda: os.getenv("S3_BASE_PREFIX", default_config.S3_BASE_PREFIX))
    # AWS credentials - loaded from .env file, then env vars, then defaults
    AWS_ACCESS_KEY_ID: Optional[str] = Field(
        default_factory=lambda: os.getenv("AWS_ACCESS_KEY_ID", default_config.AWS_ACCESS_KEY_ID) or None
    )
    AWS_SECRET_ACCESS_KEY: Optional[str] = Field(
        default_factory=lambda: os.getenv("AWS_SECRET_ACCESS_KEY", default_config.AWS_SECRET_ACCESS_KEY) or None
    )
    AWS_SESSION_TOKEN: Optional[str] = Field(
        default_factory=lambda: os.getenv("AWS_SESSION_TOKEN", default_config.AWS_SESSION_TOKEN) or None
    )
    # Optional: Add additional S3 options if needed
    # e.g., region_name, endpoint_url for specific AWS config
    s3_options: Optional[Dict[str, Any]] = None


class MockDataPaths(BaseModel):
    mock_catchment_data: str = Field(
        default_factory=lambda: os.getenv("MOCK_CATCHMENT_DATA", default_config.MOCK_CATCHMENT_DATA)
    )
    polygon_data_file: str = Field(
        default_factory=lambda: os.getenv("POLYGON_DATA_FILE", default_config.POLYGON_DATA_FILE),
        description="Path to polygon GeoDataFrame file (gpkg format)",
    )
    mock_stac_results: Optional[str] = Field(
        default_factory=lambda: os.getenv("MOCK_STAC_RESULTS", default_config.MOCK_STAC_RESULTS) or None,
        description="Path to mock STAC query results JSON",
    )
    huc: Optional[str] = Field(
        default_factory=lambda: os.getenv("HUC", default_config.HUC) or None,
        description="HUC code for mock polygon data",
    )


class HandIndexConfig(BaseModel):
    partitioned_base_path: str = Field(
        default_factory=lambda: os.getenv(
            "HAND_INDEX_PARTITIONED_BASE_PATH", default_config.HAND_INDEX_PARTITIONED_BASE_PATH
        ),
        description="Base path to partitioned parquet files (local or s3://)",
    )
    overlap_threshold_percent: float = Field(
        default_factory=lambda: float(
            os.getenv("HAND_INDEX_OVERLAP_THRESHOLD_PERCENT", str(default_config.HAND_INDEX_OVERLAP_THRESHOLD_PERCENT))
        ),
        ge=0.0,
        le=100.0,
        description="Minimum overlap percentage to keep a catchment",
    )
    enabled: bool = Field(
        default_factory=lambda: os.getenv("HAND_INDEX_ENABLED", str(default_config.HAND_INDEX_ENABLED)).lower()
        in ("true", "1", "yes", "on"),
        description="Whether to use real hand index queries (True) or mock data (False)",
    )


class StacConfig(BaseModel):
    api_url: str = Field(
        default_factory=lambda: os.getenv("STAC_API_URL", default_config.STAC_API_URL), description="STAC API root URL"
    )
    collections: List[str] = Field(
        default_factory=lambda: os.getenv("STAC_COLLECTIONS", ",".join(default_config.STAC_COLLECTIONS)).split(",")
        if os.getenv("STAC_COLLECTIONS", ",".join(default_config.STAC_COLLECTIONS))
        else [],
        description="List of STAC collection IDs to query",
    )
    overlap_threshold_percent: float = Field(
        default_factory=lambda: float(
            os.getenv("STAC_OVERLAP_THRESHOLD_PERCENT", str(default_config.STAC_OVERLAP_THRESHOLD_PERCENT))
        ),
        ge=0.0,
        le=100.0,
        description="Minimum overlap percentage to keep a STAC item",
    )
    datetime_filter: Optional[str] = Field(
        default_factory=lambda: os.getenv("STAC_DATETIME_FILTER", default_config.STAC_DATETIME_FILTER) or None,
        description="STAC datetime or interval filter",
    )
    enabled: bool = Field(
        default_factory=lambda: os.getenv("STAC_ENABLED", str(default_config.STAC_ENABLED)).lower()
        in ("true", "1", "yes", "on"),
        description="Whether to use STAC queries for flow scenarios",
    )


class FlowScenarioConfig(BaseModel):
    output_dir: str = Field(
        default_factory=lambda: os.getenv("FLOW_SCENARIOS_OUTPUT_DIR", default_config.FLOW_SCENARIOS_OUTPUT_DIR),
        description="Directory to save combined flowfiles",
    )


class WbdConfig(BaseModel):
    gpkg_path: str = Field(
        default_factory=lambda: os.getenv("WBD_GPKG_PATH", default_config.WBD_GPKG_PATH),
        description="Path to WBD_National.gpkg file",
    )
    huc_list_path: str = Field(
        default_factory=lambda: os.getenv("WBD_HUC_LIST_PATH", default_config.WBD_HUC_LIST_PATH),
        description="Path to huc_list.txt file",
    )


class Defaults(BaseModel):
    fim_type: Literal["extent", "depth"] = Field(default_factory=lambda: os.getenv("FIM_TYPE", default_config.FIM_TYPE))
    http_connection_limit: int = Field(
        default_factory=lambda: int(os.getenv("HTTP_CONNECTION_LIMIT", str(default_config.HTTP_CONNECTION_LIMIT))),
        gt=0,
        description="Max concurrent outgoing HTTP connections",
    )


class AppConfig(BaseModel):
    nomad: NomadConfig = Field(default_factory=NomadConfig)
    jobs: JobNames = Field(default_factory=JobNames)
    s3: S3Config = Field(default_factory=S3Config)
    mock_data_paths: MockDataPaths = Field(default_factory=MockDataPaths)
    hand_index: HandIndexConfig = Field(default_factory=HandIndexConfig)
    stac: Optional[StacConfig] = Field(
        default_factory=lambda: StacConfig()
        if os.getenv("STAC_ENABLED", str(default_config.STAC_ENABLED)).lower() in ("true", "1", "yes", "on")
        else None,
        description="STAC API configuration",
    )
    flow_scenarios: Optional[FlowScenarioConfig] = Field(
        default_factory=lambda: FlowScenarioConfig() if os.getenv("FLOW_SCENARIOS_OUTPUT_DIR") else None,
        description="Flow scenario processing configuration",
    )
    wbd: Optional[WbdConfig] = Field(
        default_factory=lambda: WbdConfig() if os.getenv("WBD_GPKG_PATH") or os.getenv("WBD_HUC_LIST_PATH") else None,
        description="WBD National data configuration",
    )
    defaults: Defaults = Field(default_factory=Defaults)


def load_config() -> AppConfig:
    """
    Loads and validates the application configuration.

    Configuration hierarchy (first found wins):
    1. .env file (if present)
    2. Environment variables
    3. default_config.py (fallback defaults)

    Returns:
        A validated AppConfig object.

    Raises:
        ValidationError: If the configuration data fails Pydantic validation.
        Exception: For other unexpected errors during loading.
    """
    # Load .env file first (if it exists) - these will override shell env vars
    load_dotenv(override=True)

    try:
        # Create config using default factories (env vars and defaults)
        # Pass an empty dict to force all values to come from default_factory
        config = AppConfig.model_validate({})
        logging.info("Configuration loaded using environment variables and defaults")
        return config
    except ValidationError as e:
        # Log the detailed validation errors
        error_details = e.errors()
        logging.error("Configuration validation failed:")
        for error in error_details:
            loc = " -> ".join(map(str, error["loc"]))
            logging.error(f"  - Field: '{loc}' - {error['msg']} (value: {error.get('input')})")
        raise  # Re-raise the validation error
    except Exception as e:
        logging.error(f"Unexpected error loading config: {e}")
        raise
