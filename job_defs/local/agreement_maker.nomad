job "agreement_maker" {
  datacenters = ["dc1"] 
  type        = "batch"

  parameterized {
    meta_required = [
      "pipeline_id",
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
    # Don't attempt restart since don't want to retry on most errors
    restart {
      attempts = 0
      mode     = "fail"
    }

    task "processor" {
      driver = "docker"

      config {
        image = "registry.sh.nextgenwaterprediction.com/ngwpc/fim-c/flows2fim_extents:autoeval-jobs-gval-v0.2" 
        force_pull = true

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
          "--clip_geoms", "${NOMAD_META_clip_geoms}",
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
        
        # Processing Configuration
        DASK_CLUST_MAX_MEM = "12GB"
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
        memory = 12000 # Higher memory for large raster processing
      }

      logs {
        max_files     = 5
        max_file_size = 10 # MB
      }
    }
  }
}
