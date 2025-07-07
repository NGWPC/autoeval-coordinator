datacenter = "dc1"
data_dir = "/nomad/data/"
bind_addr = "0.0.0.0"

server {
  enabled = true
  bootstrap_expect = 1
}

client {
  enabled = true
  cgroup_parent = "nomad"
}

advertise {
  http = "{{ GetPrivateIP }}:4646"
  rpc = "{{ GetPrivateIP }}:4647"
  serf = "{{ GetPrivateIP }}:4648"
}

ports {
  http = 4646
  rpc = 4647
  serf = 4648
}

ui {
  enabled = true
}

log_level = "INFO"
enable_debug = true