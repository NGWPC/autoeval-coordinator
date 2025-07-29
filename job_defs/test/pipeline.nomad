job "pipeline" {
  datacenters = ["dc1"] 
  type        = "batch"
  priority    = 75 # this needs to be within a delta of 10 of the individual pipeline stage job priority so that those don't preempt running pipeline jobs. It should still be lower so that stage jobs get scheduling preferences

  parameterized {
    meta_required = [
      "aoi",              
      "outputs_path",     
      "hand_index_path",
      "nomad_token",     # Required for test environment
    ]
    meta_optional = [
      "benchmark_sources",# Comma-separated list 
      "fim_type",         # extent or depth (default: extent)
      "registry_token",   # Required if using private registry
      "aws_access_key",
      "aws_secret_key", 
      "aws_session_token",
      "stac_datetime_filter", 
      "tags",            # Space-separated list of key=value pairs
    ]
  }

  group "pipeline-coordinator" {
    reschedule {
      attempts = 10
      interval = "24h"
      delay = "3m"
      delay_function = "constant"
      unlimited = false
    }

    restart {
      attempts = 4        # Try 4 times on the same node
      interval = "10m"    # Within a 10 minute window
      delay    = "45s"    # Wait 45s between attempts
      mode     = "fail"   # Fail after attempts exhausted
    }

    task "coordinator" {
      driver = "docker"

      config {
        image = "registry.sh.nextgenwaterprediction.com/ngwpc/fim-c/flows2fim_extents:autoeval-coordinator-v0.1"
        # force_pull = false
        force_pull = true # use a cached image on client if available. To force a pull need to change back to force_pull = true
        network_mode = "host"
        
        # Docker registry authentication
        auth {
          username = "ReadOnly_NGWPC_Group_Deploy_Token"
          password = "${NOMAD_META_registry_token}"
        }

        args = [
          "--aoi", "${NOMAD_META_aoi}",
          "--outputs_path", "${NOMAD_META_outputs_path}",
          "--hand_index_path", "${NOMAD_META_hand_index_path}",
          "--benchmark_sources", "${NOMAD_META_benchmark_sources}",
          "--tags", "${NOMAD_META_tags}",
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
        # Pipeline ID (using Nomad job ID)
        NOMAD_PIPELINE_JOB_ID = "${NOMAD_JOB_ID}"
        
        # AWS Configuration
        # Test nomad clients can use IAM
        AWS_DEFAULT_REGION    = "us-east-1"      
        # AWS_ACCESS_KEY_ID     = "${NOMAD_META_aws_access_key}"
        # AWS_SECRET_ACCESS_KEY = "${NOMAD_META_aws_secret_key}"
        # AWS_SESSION_TOKEN     = "${NOMAD_META_aws_session_token}"

        # Nomad Configuration
        NOMAD_ADDRESS         = "http://nomad-server-test.test.nextgenwaterprediction.com:4646/"
        NOMAD_TOKEN           = "${NOMAD_META_nomad_token}" # Changed to use meta parameter for test
        NOMAD_NAMESPACE       = "default"
        NOMAD_REGISTRY_TOKEN        = "${NOMAD_META_registry_token}"
 
        # Pipeline Configuration
        FIM_TYPE              = "extent"
        HTTP_CONNECTION_LIMIT = "100"
        
        # HAND Index Configuration
        HAND_INDEX_OVERLAP_THRESHOLD_PERCENT = "40.0"
        
        # STAC Configuration
        STAC_API_URL            = "http://benchmark-stac.test.nextgenwaterprediction.com:8000/" # Using production STAC API for test
        STAC_OVERLAP_THRESHOLD_PERCENT = "40.0"
        STAC_DATETIME_FILTER  = "${NOMAD_META_stac_datetime_filter}"
        
        # Job Names for dispatching child jobs
        HAND_INUNDATOR_JOB_NAME = "hand_inundator"
        FIM_MOSAICKER_JOB_NAME  = "fim_mosaicker"
        AGREEMENT_MAKER_JOB_NAME = "agreement_maker"
        
        FLOW_SCENARIOS_OUTPUT_DIR = "combined_flowfiles"
   
        LOG_LEVEL             = "INFO"
        PYTHONUNBUFFERED      = "1"
      }

      resources {
        memory = 4000 
      }

      logs {
        max_files     = 5
        max_file_size = 20 # MB
      }
    }
  }
}
