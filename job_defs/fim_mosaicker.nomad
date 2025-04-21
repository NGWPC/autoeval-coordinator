job "fim_mosaicker" {
  datacenters = ["dc1"] 
  type        = "batch"

  constraint {
    attribute = "${node.class}"
    value     = "linux"
  }

  parameterized {
    meta_required = [
      "pipeline_id",
      "raster_paths", 
      "output_path",
      "fim_type",
      "geo_mem_cache",
    ]
    meta_optional = [
      "registry_token", # Required if using private registry auth below
      "aws_access_key",
      "aws_secret_key",
      "aws_session_token",
    ]
  }

  group "mosaicker-processor" {
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
          username = "ReadOnly_NGWPC_Group_Deploy_Token"
          password = "${NOMAD_META_registry_token}"
        }

        command = "python3"
        args = [
          "/app/fim_mosaicker/mosaic.py",
          "--raster_paths", "${NOMAD_META_raster_paths}",
          "--output_path", "${NOMAD_META_output_path}",
          "--fim-type", "${NOMAD_META_fim_type}",
          "--geo-mem-cache", "${NOMAD_META_geo_mem_cache}",
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
        AWS_ACCESS_KEY_ID     = "${NOMAD_META_aws_access_key}"
        AWS_SECRET_ACCESS_KEY = "${NOMAD_META_aws_secret_key}"
        AWS_SESSION_TOKEN     = "${NOMAD_META_aws_session_token}"
        AWS_DEFAULT_REGION = "us-east-1"
      }

      resources {
        cpu    = 1000 
        memory = 4096 
      }

      logs {
        max_files     = 5
        max_file_size = 10 # MB
      }
    }
  }
}
