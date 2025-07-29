job "hand_inundator" {
  datacenters = ["dc1"] 
  type        = "batch"
  priority    = 80

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
    reschedule {
      attempts = 0 # this needs to only be 0 re-attempts or will mess up pipeline job tracking
    }

    restart {
      attempts = 4        # Try 4 times on the same node
      interval = "10m"    # Within a 10 minute window
      delay    = "45s"    # Wait 45s between attempts
      mode     = "fail"   # Fail after attempts exhausted
    }

    task "processor" {
      driver = "docker"

      config {
        # use last known stable version in test
        image = "registry.sh.nextgenwaterprediction.com/ngwpc/fim-c/flows2fim_extents:autoeval-jobs-v0.2" 
        # force_pull = false
        force_pull = true # use a cached image on client if available. To force a pull need to change back to force_pull = true

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
        # AWS_ACCESS_KEY_ID     = "${NOMAD_META_aws_access_key}"
        # AWS_SECRET_ACCESS_KEY = "${NOMAD_META_aws_secret_key}"
        # AWS_SESSION_TOKEN     = "${NOMAD_META_aws_session_token}"

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
        memory = 2000 # Memory in MiB 
      }

      logs {
        max_files     = 5
        max_file_size = 10 # MB
      }
    }
  }
}
