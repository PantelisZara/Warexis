#!/usr/bin/env bash
set -euo pipefail
root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)"
state="${WAREXIS_K8S_STATE_DIR:-${XDG_STATE_HOME:-$HOME/.local/state}/warexis-supervisor/phase11/kind}"
tools="${WAREXIS_K8S_TOOLS_DIR:-${XDG_DATA_HOME:-$HOME/.local/share}/warexis/kubernetes/bin}"
export PATH="$tools:$PATH"
config="$state/kubeconfig"
if [[ $# != 1 || "$1" != apply ]]; then
  echo 'Usage: deploy.sh apply (only the dedicated kind-warexis-phase11 context)' >&2; exit 2
fi
[[ -f "$config" ]] || { echo 'Create the dedicated cluster first.' >&2; exit 1; }
k=(kubectl --kubeconfig "$config" --context kind-warexis-phase11)
[[ "$("${k[@]}" config current-context)" == kind-warexis-phase11 ]] || exit 1
# Refuse to apply against an unrelated context/cluster, even if a kubeconfig is replaced.
[[ "$("${k[@]}" get node warexis-phase11-control-plane -o jsonpath='{.metadata.labels.kind\.x-k8s\.io/cluster}')" == warexis-phase11 ]] || {
  # kind does not guarantee a cluster-name node label; verify the unique node identity as well.
  [[ "$("${k[@]}" get nodes -o jsonpath='{.items[*].metadata.name}')" == warexis-phase11-control-plane ]] || exit 1
}
python3 "$root/infrastructure/kubernetes/scripts/secrets.py" "$state/secrets"
"${k[@]}" apply -f "$root/infrastructure/kubernetes/namespace.yaml"
# Pipe generated manifests directly. Server-side apply avoids a second last-applied credential copy.
"${k[@]}" -n warexis create secret generic warexis-runtime --from-env-file="$state/secrets/runtime.env" --dry-run=client -o json |
  "${k[@]}" -n warexis apply --server-side --field-manager=warexis-local -f -
"${k[@]}" -n warexis create configmap warexis-realm --from-file=warexis-realm.json="$root/infrastructure/auth/warexis-realm.json" --dry-run=client -o yaml |
  "${k[@]}" -n warexis apply -f -
"${k[@]}" apply --dry-run=server -k "$root/infrastructure/kubernetes"
"${k[@]}" apply -k "$root/infrastructure/kubernetes"
for resource in statefulset/order-postgres statefulset/inventory-postgres statefulset/fulfillment-postgres statefulset/keycloak-postgres statefulset/kafka deployment/keycloak deployment/order deployment/inventory deployment/fulfillment deployment/frontend; do
  "${k[@]}" -n warexis rollout status "$resource" --timeout=600s
done
"${k[@]}" -n warexis get pods,services,pvc
