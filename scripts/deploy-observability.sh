#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <node-ip>" >&2
  exit 2
fi

node_ip="$1"
ssh_target="root@${node_ip}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ssh_opts=(-F /dev/null -i /home/linux/.ssh/id_ed25519 -o StrictHostKeyChecking=accept-new)

ssh "${ssh_opts[@]}" "$ssh_target" 'kubectl create namespace observability --dry-run=client -o yaml | kubectl apply -f -'

# The password is generated and stored only as a Kubernetes Secret; it never
# appears in the repository or command output. Anonymous access is Viewer-only.
ssh "${ssh_opts[@]}" "$ssh_target" 'kubectl -n observability get secret grafana-admin >/dev/null 2>&1 || kubectl -n observability create secret generic grafana-admin --from-literal=password="$(openssl rand -hex 24)"'

ssh "${ssh_opts[@]}" "$ssh_target" 'kubectl -n observability create configmap grafana-dashboard --from-file=duvo-overview.json=/dev/stdin --dry-run=client -o yaml | kubectl apply -f -' < "$repo_root/dashboards/duvo-overview.json"
ssh "${ssh_opts[@]}" "$ssh_target" 'kubectl apply -f -' < "$repo_root/deploy/observability/stack.yaml"
ssh "${ssh_opts[@]}" "$ssh_target" 'kubectl -n observability rollout restart deployment/victoriametrics deployment/vmalert deployment/grafana'

for workload in victoriametrics redis-exporter kube-state-metrics alertmanager vmalert grafana; do
  ssh "${ssh_opts[@]}" "$ssh_target" "kubectl -n observability rollout status deployment/${workload} --timeout=300s"
done
ssh "${ssh_opts[@]}" "$ssh_target" 'kubectl -n observability rollout status daemonset/node-exporter --timeout=300s'

echo "Grafana: http://${node_ip}/grafana/ (anonymous Viewer access enabled)"
