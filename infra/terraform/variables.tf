variable "name" {
  description = "Prefix used for Hetzner resources."
  type        = string
  default     = "sre-sandbox"
}

variable "location" {
  description = "Hetzner location for the node."
  type        = string
  default     = "nbg1"
}

variable "server_type" {
  description = "Node size; 8 GB leaves room for the later observability stack."
  type        = string
  default     = "cpx32"
}

variable "ssh_public_key_path" {
  description = "Public key that is allowed to log in as root."
  type        = string
  default     = "/home/linux/.ssh/id_ed25519.pub"
}

variable "ssh_allowed_cidrs" {
  description = "Networks allowed to reach SSH. Narrow this in a real deployment."
  type        = list(string)
  default     = ["0.0.0.0/0", "::/0"]
}
