# Lightweight sandbox orchestrator

This repository provisions one Hetzner Cloud node, installs k3s, and deploys a
minimal Redis Streams producer/consumer. The consumer creates one HTTP sandbox
Pod, Service, and Ingress per job.

## Provision and deploy

```bash
export HCLOUD_TOKEN="$(tr -d '\n' </tmp/hetzner-key)"
make apply
make deploy
```

Submit a job using the `producer_url` Terraform output:

```bash
curl -X POST "http://NODE_IP/api/jobs" \
  -H 'content-type: application/json' \
  -d '{"jobId":"abc123","type":"http"}'
curl "http://NODE_IP/sandboxes/abc123"
```

SSH access uses the existing `/home/linux/.ssh/id_ed25519` key:

```bash
ssh root@NODE_IP
kubectl get pods -A
```

## Deliberate bootstrap shortcuts

- This is a single-node cluster and is not highly available.
- Python source is mounted through a ConfigMap, and dependencies are installed
  on container startup. A real build should publish an immutable application image.
- SSH is key-only but initially open to all source addresses. Set
  `ssh_allowed_cidrs` before using this beyond the interview environment.
- Redis uses the local-path provisioner. It survives Pod restarts, not node loss.
- Kubernetes containers are not a sufficient security boundary for hostile code.
- Failed stream entries remain pending for inspection; bounded retry and a DLQ
  belong in the next iteration.
