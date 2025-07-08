job "pipeline" {
  datacenters = ["dc1"] 
  type        = "batch"

  parameterized {
    meta_required = [
      "aoi",              
      "outputs_path",     
      "hand_index_path",  
    ]
    meta_optional = [
      "benchmark_sources",# Comma-separated list 
      "fim_type",         # extent or depth (default: extent)
      "registry_token",   # Required if using private registry
      "aws_access_key",
      "aws_secret_key", 
      "aws_session_token",
      "stac_datetime_filter", 
    ]
  }

  group "pipeline-coordinator" {
    restart {
      attempts = 0
      mode     = "fail"
    }

    task "coordinator" {
      driver = "docker"

      config {
        image = "registry.sh.nextgenwaterprediction.com/ngwpc/fim-c/flows2fim_extents:autoeval-coordinator-v0.1" 
        force_pull = true
        network_mode = "host"
        
        auth {
          username = "ReadOnly_NGWPC_Group_Deploy_Token"
          password = "${NOMAD_META_registry_token}"
        }

        args = [
          "--aoi", "${NOMAD_META_aoi}",
          "--outputs_path", "${NOMAD_META_outputs_path}",
          "--hand_index_path", "${NOMAD_META_hand_index_path}",
          "--benchmark_sources", "${NOMAD_META_benchmark_sources}",
        ]
      }

      env {
        # Pipeline ID (using Nomad job ID)
        NOMAD_PIPELINE_JOB_ID = "${NOMAD_JOB_ID}"
        
        # AWS Configuration
        AWS_ACCESS_KEY_ID     = "${NOMAD_META_aws_access_key}"
        AWS_SECRET_ACCESS_KEY = "${NOMAD_META_aws_secret_key}"
        AWS_SESSION_TOKEN     = "${NOMAD_META_aws_session_token}"
        AWS_DEFAULT_REGION    = "us-east-1"
        
        # Nomad Configuration
        NOMAD_ADDRESS         = "http://127.0.0.1:4646"
        NOMAD_TOKEN           = "${NOMAD_TOKEN}"
        NOMAD_NAMESPACE       = "default"
        NOMAD_REGISTRY_TOKEN  = "${NOMAD_META_registry_token}"
        
        # S3 Configuration
        S3_BUCKET             = "fimc-data"
        S3_BASE_PREFIX        = "${NOMAD_META_output_path}"
        
        # Pipeline Configuration
        FIM_TYPE              = "extent"
        HTTP_CONNECTION_LIMIT = "100"
        
        # HAND Index Configuration
        HAND_INDEX_PARTITIONED_BASE_PATH = "s3://fimc-data/autoeval/hand_output_indices/PI3_uat_and_alpha_domain_3m_wbt/"
        HAND_INDEX_OVERLAP_THRESHOLD_PERCENT = "40.0"
        
        # STAC Configuration
        STAC_API_URL          = "http://127.0.0.1:8082/"
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
        memory = 8192 
      }

      logs {
        max_files     = 5
        max_file_size = 20 # MB
      }
    }
  }
}
