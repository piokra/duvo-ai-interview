output "server_ip" {
  description = "Public IPv4 address of the k3s node."
  value       = hcloud_server.node.ipv4_address
}

output "ssh_command" {
  description = "Command for operator access."
  value       = "ssh root@${hcloud_server.node.ipv4_address}"
}

output "producer_url" {
  value = "http://${hcloud_server.node.ipv4_address}/api/jobs"
}
