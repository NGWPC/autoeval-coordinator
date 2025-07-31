job "agreement_maker" {
  datacenters = ["dc1"] 
  type        = "batch"
  priority    = 80

  parameterized {
    meta_required = [
      "candidate_path", 
      "benchmark_path",
      "output_path",
      "fim_type",
    ]
    meta_optional = [
      "metrics_path",
      "clip_geoms",
      "registry_token", # Required if using private registry 
      "aws_access_key",
      "aws_secret_key",
      "aws_session_token",
    ]
  }

  group "agreement-processor" {
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
        image = "registry.sh.nextgenwaterprediction.com/ngwpc/fim-c/flows2fim_extents:autoeval-jobs-gval-v0.2" 
        force_pull = false
        # force_pull = true # use a cached image on client if available. To force a pull need to change back to force_pull = true

        auth {
          username = "ReadOnly_NGWPC_Group_Deploy_Token" # Or your specific username
          password = "${NOMAD_META_registry_token}"
        }
        command = "python3"
        args = [
          "/deploy/agreement_maker/make_agreement.py",
          "--fim_type", "${NOMAD_META_fim_type}",
          "--candidate_path", "${NOMAD_META_candidate_path}",
          "--benchmark_path", "${NOMAD_META_benchmark_path}",
          "--output_path", "${NOMAD_META_output_path}",
          "--metrics_path", "${NOMAD_META_metrics_path}",
          "--mask_dict", "s3://fimc-data/autoeval/test_data/mask_areas.json",
        ]

        logging {
          type = "awslogs"
          config {
            awslogs-group        = "/aws/ec2/nomad-client-linux-test"
            awslogs-region       = "us-east-1"
            awslogs-stream       = "${NOMAD_JOB_ID}"
            awslogs-create-group = "true"
          }
        }
      }

      env {
        # AWS_ACCESS_KEY_ID     = "${NOMAD_META_aws_access_key}"
        # AWS_SECRET_ACCESS_KEY = "${NOMAD_META_aws_secret_key}"
        # AWS_SESSION_TOKEN     = "${NOMAD_META_aws_session_token}"
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
        
        # Processing Configuration
        DASK_CLUST_MAX_MEM = "8GB"
        RASTERIO_CHUNK_SIZE = "4096"
        DEFAULT_WRITE_BLOCK_SIZE = "4096"
        COG_BLOCKSIZE = "512"
        COG_OVERVIEW_LEVEL = "4"
        
        # Nodata Values
        EXTENT_NODATA_VALUE = "255"
        
        # Processing Defaults
        DEFAULT_CLIP_OPERATION = "exclude"
        
        # Logging
        LOG_SUCCESS_LEVEL_NUM = "25"
      }

      resources {
        memory = 8000 # Higher memory for large raster processing
      }

      logs {
        max_files     = 5
        max_file_size = 10 # MB
      }
    }
  }
}
