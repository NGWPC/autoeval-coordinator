job "inundation-processor" {
  parameterized {
    meta_required = [
      "forecast_path",
      "output_path", 
      "catchment_id",
      "catchment_data_path"
    ]
    meta_optional = ["window_size"]
  }

  datacenters = ["dc1"]
  type        = "batch"

  group "inundation" {
    task "process" {
      driver = "docker"

      config {
        image = "inundation-processor:v1"
        
        # Mount AWS credentials
        volumes = [
          "/home/dylan.lee/.aws:/root/.aws:ro"
        ]
        
        args  = [
          "--catchment-data", "${NOMAD_META_catchment_data_path}",
          "--forecast-path", "${NOMAD_META_forecast_path}",
          "--output-path", "${NOMAD_META_output_path}",
          "--catchment-id", "${NOMAD_META_catchment_id}",
          "--window-size", "${NOMAD_META_window_size}"
        ]
      }

      resources {
        cpu    = 1000
        memory = 2048
      }
    }
  }
}
