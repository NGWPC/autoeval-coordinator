job "hand_inundator" {
  datacenters = ["dc1"] 
  type        = "batch"

  constraint {
    attribute = "${node.class}"
    value     = "linux"
  }

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
        # use last known stable version in test
        image = "registry.sh.nextgenwaterprediction.com/ngwpc/fim-c/flows2fim_extents:autoeval-jobs-v0.2" 
        force_pull = true

        auth {
          username = "ReadOnly_NGWPC_Group_Deploy_Token" # Or your specific username
          password = "${NOMAD_META_registry_token}"
        }
        command = "python3"
        args = [
          "/deploy/hand_inundator/inundate.py",
          "--catchment_data_path", "${NOMAD_META_catchment_data_path}",
          "--forecast_path", "${NOMAD_META_forecast_path}",
          "--fim_output_path", "${NOMAD_META_output_path}",
          "--fim_type", "${NOMAD_META_fim_type}",
        ]

        logging {
          type = "awslogs" # Assumes AWS Logs driver setup on clients
          config {
            awslogs-group        = "/aws/ec2/nomad-client-linux-test"
            awslogs-region       = "us-east-1"
            awslogs-stream       = "${NOMAD_JOB_ID}"
            awslogs-create-group = "true"
          }
        }
      }

      env {
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
        cpu    = 1000 # CPU in MHz (example: 1000 = 1 GHz)
        memory = 2048 # Memory in MiB 
      }

      logs {
        max_files     = 5
        max_file_size = 10 # MB
      }
    }
  }
}
