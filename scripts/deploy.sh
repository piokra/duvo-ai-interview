#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <node-ip>" >&2
  exit 2
fi

node_ip="$1"
ssh_target="root@${node_ip}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_manifest="$(mktemp)"
trap 'rm -f "$tmp_manifest"' EXIT

sed "s|__PUBLIC_BASE_URL__|http://${node_ip}|g" "$repo_root/deploy/platform.yaml" > "$tmp_manifest"

ssh -o StrictHostKeyChecking=accept-new "$ssh_target" 'kubectl create namespace platform --dry-run=client -o yaml | kubectl apply -f -'
scp "$repo_root/services/producer.py" "$repo_root/services/consumer.py" "$repo_root/services/requirements.txt" "${ssh_target}:/tmp/"
ssh "$ssh_target" 'kubectl -n platform create configmap platform-code --from-file=/tmp/producer.py --from-file=/tmp/consumer.py --from-file=/tmp/requirements.txt --dry-run=client -o yaml | kubectl apply -f -'
ssh "$ssh_target" 'kubectl apply -f -' < "$tmp_manifest"
ssh "$ssh_target" 'kubectl -n platform rollout restart deployment/producer deployment/consumer && kubectl -n platform rollout status deployment/redis --timeout=180s && kubectl -n platform rollout status deployment/producer --timeout=300s && kubectl -n platform rollout status deployment/consumer --timeout=300s'
