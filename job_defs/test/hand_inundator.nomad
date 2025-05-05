job "hand_inundator" {
  datacenters = ["dc1"] 
  type        = "batch"

  constraint {
    attribute = "${node.class}"
    value     = "linux"
  }

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
        image = "registry.sh.nextgenwaterprediction.com/ngwpc/fim-c/flows2fim_extents:autoeval-jobs" 
        force_pull = true

        auth {
          username = "ReadOnly_NGWPC_Group_Deploy_Token" # Or your specific username
          password = "${NOMAD_META_registry_token}"
        }
        command = "python3"
        args = [
          "/deploy/hand_inundator/inundate.py",
          "--catchment-data", "${NOMAD_META_catchment_data_path}",
          "--forecast-path", "${NOMAD_META_forecast_path}",
          "--output-path", "${NOMAD_META_output_path}",
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

      # --- Environment Variables (for AWS SDK inside container) ---
      # Pass AWS creds if provided in meta, otherwise rely on IAM instance profile
      env {
        AWS_DEFAULT_REGION = "us-east-1"
        GDAL_CACHEMAX         = "${NOMAD_META_gdal_cache_max}"
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
