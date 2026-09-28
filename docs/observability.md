# Observability

The interview deployment uses a compact, single-node monitoring stack:

- VictoriaMetrics scrapes and retains metrics for 14 days.
- Grafana serves a provisioned bird's-eye dashboard at `/grafana/`.
- `vmalert` evaluates version-controlled rules and sends firing alerts to a
  local Alertmanager.
- Redis Exporter reports queue/stream state, kube-state-metrics reports
workload state, and node-exporter reports host capacity.

The dashboard includes produced counts, successful completion rate, batch
processing p95, and live Kubernetes Job success counts grouped by workload and
manifest version. It also plots canary traffic weight, observed error ratio,
and rollout state so the automated rollback remains visible. The default
`load-generator` emits an aggregate 10 jobs per second.

Deploy or update it with:

```bash
./scripts/deploy-observability.sh NODE_IP
```

The Grafana admin password is generated directly on the node and stored only in
the `grafana-admin` Kubernetes Secret. Anonymous users have Viewer access. To
retrieve the admin password over an established SSH session:

```bash
kubectl -n observability get secret grafana-admin \
  -o jsonpath='{.data.password}' | base64 -d
```

## Coverage

The dashboard and alerts cover job ingress and completion, errors, Redis stream
length, sandbox startup latency and pod phases, scrape health, pod restarts,
node readiness, memory, and disk capacity.

## Deliberate shortcuts

- Alertmanager has a local sink because no external paging destination was
  supplied. Rules and alert state are live, but notifications are not delivered
  off-node. In production, route critical alerts to PagerDuty or Slack.
- Metrics and their storage run on the monitored node. A node loss therefore
  removes both the service and its telemetry; production monitoring should be
  external or highly available.
- Static scrape targets keep the deployment small and legible. Service
  discovery or the VictoriaMetrics Operator is a better fit as service count
  grows.
- Logs remain structured stdout accessible through Kubernetes. A log backend
  was deferred to keep memory use sensible on the single interview node.
- Dashboard counters use Prometheus counter deltas, so values reset with the
  ephemeral Python application pods. Publishing immutable images and preserving
  deployment stability would reduce those discontinuities.
