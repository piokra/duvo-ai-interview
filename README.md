# Lightweight sandbox orchestrator

This repository provisions one Hetzner Cloud node, installs k3s, and deploys a
minimal Redis Streams producer/consumer. The consumer creates one HTTP sandbox
Pod, Service, and Ingress per job from a trusted, versioned workload catalog.

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
  -d '{"jobId":"abc123","workload":{"name":"http-echo","version":"1.0.0"}}'
curl "http://NODE_IP/sandboxes/abc123"
```

The second built-in workload is a request inspector, useful for checking the
network identity and headers visible inside a sandbox:

```bash
curl "http://NODE_IP/api/workloads"
curl -X POST "http://NODE_IP/api/jobs" \
  -H 'content-type: application/json' \
  -d '{"jobId":"inspect-001","workload":{"name":"request-inspector","version":"1.0.0"}}'
curl "http://NODE_IP/sandboxes/inspect-001"
```

## Versioned workload manifests

[`manifests/workloads.json`](manifests/workloads.json) is the allow-listed
workload catalog and [`manifests/workload.schema.json`](manifests/workload.schema.json)
documents its JSON Schema. Each manifest has a stable `(metadata.name,
metadata.version)` identity and contains only a pinned image tag, arguments,
port, resource boundaries, and readiness path. To change a workload, add a new
semantic version rather than editing an already deployed version.

The producer resolves an API reference against this catalog and puts the
identity plus a canonical SHA-256 manifest digest on Redis. The consumer
resolves the same version locally and refuses a job if the digest differs.
This makes producer/consumer catalog skew fail closed and leaves
`workload-version` labels on Kubernetes objects and metrics for future rollout
targeting. Arbitrary images, commands, environment variables, volumes, and
security contexts cannot be submitted through the public API.

Sandbox Pods also disable service-account token mounting, drop Linux
capabilities, use the runtime-default seccomp profile, forbid privilege
escalation, run as non-root, and use a read-only root filesystem. This reduces
risk but is not a hardened multi-tenant sandbox boundary.

Run the focused catalog checks without third-party test dependencies:

```bash
python3 -m unittest discover -s tests -v
```

## Observability

Deploy the metrics, dashboard, and alerting stack with:

```bash
./scripts/deploy-observability.sh NODE_IP
```

Grafana is served at `http://NODE_IP/grafana/` with anonymous Viewer access.
VictoriaMetrics retains 14 days of application, queue, Kubernetes, and node
metrics; `vmalert` evaluates the version-controlled rules in
[`deploy/observability/stack.yaml`](deploy/observability/stack.yaml). See
[`docs/observability.md`](docs/observability.md) for coverage and shortcuts.

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
