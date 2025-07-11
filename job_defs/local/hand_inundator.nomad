variable "repo_root" {
  description = "Path to the repository root directory"
  type        = string
}

job "hand_inundator" {
  datacenters = ["dc1"] 
  type        = "batch"

  parameterized {
    meta_required = [
      "catchment_data_path",
      "forecast_path",
      "output_path",
    ]
    meta_optional = [
      "fim_type", 
      "registry_token", # Required if using private registry 
      "aws_access_key",
      "aws_secret_key",
      "aws_session_token",
    ]
  }

  group "inundator-processor" {
    # Don't attempt restart since don't want to retry on most errors
    restart {
      attempts = 0
      mode     = "fail"
    }

    task "processor" {
      driver = "docker"

      config {
        # Use local development image - must use specific tag (not 'latest')
        # to prevent Nomad from trying to pull from a registry
        image = "autoeval-jobs:local" 
        force_pull = false
        
        # Mount local test data and output directory
        volumes = [
          "${var.repo_root}/test:/test:ro",
          "/tmp/autoeval-outputs:/outputs:rw",
          "/tmp:/tmp:rw"
        ]
        
        command = "python3"
        args = [
          "/deploy/hand_inundator/inundate.py",
          "--catchment_data_path", "${NOMAD_META_catchment_data_path}",
          "--forecast_path", "${NOMAD_META_forecast_path}",
          "--fim_output_path", "${NOMAD_META_output_path}",
          "--fim_type", "${NOMAD_META_fim_type}",
        ]

      }

      # --- Environment Variables (for AWS SDK inside container) ---
      # Pass AWS creds if provided in meta, otherwise rely on IAM instance profile
      env {
        AWS_ACCESS_KEY_ID     = "${NOMAD_META_aws_access_key}"
        AWS_SECRET_ACCESS_KEY = "${NOMAD_META_aws_secret_key}"
        AWS_SESSION_TOKEN     = "${NOMAD_META_aws_session_token}"
        AWS_DEFAULT_REGION = "us-east-1"
        GDAL_CACHEMAX         = "1024"
        
        # GDAL Configuration
        GDAL_NUM_THREADS = "1"
        GDAL_TIFF_DIRECT_IO = "YES"
        GDAL_DISABLE_READDIR_ON_OPEN = "TRUE"
        CPL_LOG_ERRORS = "ON"
        CPL_VSIL_CURL_ALLOWED_EXTENSIONS = ".tif,.vrt"
        VSI_CACHE_SIZE = "268435456"
        CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE = "YES"
        
        # Processing Defaults
        LAKE_ID_FILTER_VALUE = "-999"
        
        # Nodata Values
        DEPTH_NODATA_VALUE = "-9999"
        INUNDATION_NODATA_VALUE = "255"
        
        # Output Configuration
        INUNDATION_COMPRESS_TYPE = "lzw"
        INUNDATION_BLOCK_SIZE = "256"
        
        # Logging
        LOG_SUCCESS_LEVEL_NUM = "25"
      }

      resources {
        # set small here for github runner test (8gb total memory)
        memory = 4000
      }

      logs {
        max_files     = 5
        max_file_size = 10 # MB
      }
    }
  }
}
