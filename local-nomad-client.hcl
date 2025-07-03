datacenter = "dc1"

client {
  enabled = true
  node_class = "linux"
}

plugin "docker" {
  config {
    allow_privileged = true
    volumes {
      enabled = true
    }
    extra_labels = ["job_name", "task_group_name", "task_name", "namespace"]
  }
}
