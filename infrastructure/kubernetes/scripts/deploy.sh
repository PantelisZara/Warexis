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
# Verify the recorded Docker node ID, named context and loopback API before mutation.
"$root/infrastructure/kubernetes/scripts/cluster.sh" status >/dev/null
# Never recreate credentials against existing durable DBs, or silently overwrite drift.
if [[ ! -f "$state/secrets/runtime.env" ]]; then
  existing_claims="$("${k[@]}" -n warexis get pvc -o name --ignore-not-found)"
  [[ -z "$existing_claims" ]] || {
    echo 'Durable volumes exist but private credentials are missing; restore matching inputs. Refusing new credentials.' >&2
    exit 1
  }
fi
existing_secret="$("${k[@]}" -n warexis get secret warexis-runtime --ignore-not-found -o json)"
if [[ -n "$existing_secret" ]]; then
  [[ -f "$state/secrets/runtime.env" ]] || {
    echo 'Runtime Secret exists but private input is missing; restore the matching private input. Refusing credential replacement.' >&2
    exit 1
  }
  printf '%s' "$existing_secret" | python3 -c '
import base64, json, pathlib, sys
current=json.load(sys.stdin).get("data", {})
expected=dict(line.split("=", 1) for line in pathlib.Path(sys.argv[1]).read_text().splitlines())
actual={k:base64.b64decode(v, validate=True).decode() for k,v in current.items()}
if actual != expected:
    sys.exit("Private input and existing Secret differ; inspect matching credentials privately before redeploying")
' "$state/secrets/runtime.env"
fi
unset existing_secret
python3 "$root/infrastructure/kubernetes/scripts/generate-secrets.py" "$state/secrets"
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
