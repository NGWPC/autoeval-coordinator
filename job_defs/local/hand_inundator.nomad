job "hand_inundator" {
  datacenters = ["dc1"] 
  type        = "batch"

  parameterized {
    meta_required = [
      "pipeline_id",
      "catchment_data_path",
      "forecast_path",
      "output_path",
      "gdal_cache_max",  
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

      }

      # --- Environment Variables (for AWS SDK inside container) ---
      # Pass AWS creds if provided in meta, otherwise rely on IAM instance profile
      env {
        AWS_ACCESS_KEY_ID     = "${NOMAD_META_aws_access_key}"
        AWS_SECRET_ACCESS_KEY = "${NOMAD_META_aws_secret_key}"
        AWS_SESSION_TOKEN     = "${NOMAD_META_aws_session_token}"
        AWS_DEFAULT_REGION = "us-east-1"
        GDAL_CACHEMAX         = "${NOMAD_META_gdal_cache_max}"
      }

      resources {
        cpu    = 1000 # Adjust CPU MHz (example: 1 core = 1000)
        memory = 4096 # Adjust Memory MiB (example: 4 GiB)
      }

      logs {
        max_files     = 5
        max_file_size = 10 # MB
      }
    }
  }
}
