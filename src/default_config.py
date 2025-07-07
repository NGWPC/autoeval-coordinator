"""
Default configuration values for the pipeline.
These serve as fallbacks when values are not set in .env or environment variables.
"""

# Nomad defaults
NOMAD_ADDRESS = "http://localhost:4646"
NOMAD_TOKEN = ""
NOMAD_NAMESPACE = "default"
NOMAD_REGISTRY_TOKEN = ""

# Job names
HAND_INUNDATOR_JOB_NAME = "hand_inundator"
FIM_MOSAICKER_JOB_NAME = "fim_mosaicker"
AGREEMENT_MAKER_JOB_NAME = "agreement_maker"

# AWS credentials defaults
AWS_ACCESS_KEY_ID = ""
AWS_SECRET_ACCESS_KEY = ""
AWS_SESSION_TOKEN = ""

# HAND index defaults
HAND_INDEX_PARTITIONED_BASE_PATH = "s3://fimc-data/autoeval/hand_output_indices/PI3_uat_and_alpha_domain_3m_wbt/"
HAND_INDEX_OVERLAP_THRESHOLD_PERCENT = 40.0

# STAC defaults
STAC_API_URL = "http://127.0.0.1:8082/"
STAC_OVERLAP_THRESHOLD_PERCENT = 40.0
STAC_DATETIME_FILTER = ""

# Flow scenario defaults
FLOW_SCENARIOS_OUTPUT_DIR = "combined_flowfiles"

# General defaults
FIM_TYPE = "extent"
HTTP_CONNECTION_LIMIT = 100
