resource "hcloud_ssh_key" "operator" {
  name       = "${var.name}-operator"
  public_key = file(var.ssh_public_key_path)
}

resource "hcloud_firewall" "node" {
  name = "${var.name}-firewall"

  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "22"
    source_ips = var.ssh_allowed_cidrs
  }

  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "80"
    source_ips = ["0.0.0.0/0", "::/0"]
  }

  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "443"
    source_ips = ["0.0.0.0/0", "::/0"]
  }
}

resource "hcloud_server" "node" {
  name        = var.name
  image       = "ubuntu-24.04"
  server_type = var.server_type
  location    = var.location
  ssh_keys    = [hcloud_ssh_key.operator.id]
  user_data   = file("${path.module}/cloud-init.yaml")

  firewall_ids = [hcloud_firewall.node.id]

  labels = {
    project    = "sre-sandbox"
    managed_by = "terraform"
  }

  public_net {
    ipv4_enabled = true
    ipv6_enabled = true
  }
}
