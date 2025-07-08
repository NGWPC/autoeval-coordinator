datacenter = "dc1"
data_dir = "/nomad/data/"
bind_addr = "0.0.0.0"

server {
  enabled = true
  # bootstrap_expect = 1 means this is a single-node cluster that doesn't need to wait for other servers
  bootstrap_expect = 1
}

client {
  enabled = true
  # cgroup_parent specifies the parent cgroup for all Nomad-managed processes
  # This ensures proper resource isolation and prevents conflicts with system cgroups
  cgroup_parent = "nomad"
}

# Advertise tells other Nomad agents how to reach this node
# GetPrivateIP dynamically resolves to the node's private IP address
# This is necessary for proper cluster communication in containerized environments
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
