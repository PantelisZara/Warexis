#!/usr/bin/env bash
# Only this named cluster is managed; user-global kubeconfig is never changed.
set -euo pipefail
umask 077
CLUSTER=warexis-phase11
CONTEXT=kind-warexis-phase11
NODE_IMAGE=kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5
repo=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
TOOLS_DIR=${WAREXIS_K8S_TOOLS_DIR:-${XDG_DATA_HOME:-$HOME/.local/share}/warexis/kubernetes/bin}
STATE_DIR=$(realpath -m "${WAREXIS_K8S_STATE_DIR:-${XDG_STATE_HOME:-$HOME/.local/state}/warexis-supervisor/phase11/kind}")
KUBECONFIG_FILE=$STATE_DIR/kubeconfig
KIND=$TOOLS_DIR/kind
KUBECTL=$TOOLS_DIR/kubectl
fail() { echo "$*" >&2; exit 1; }
case "$STATE_DIR/" in "$repo/"*) fail 'Cluster state and credentials must be outside the repository.' ;; esac
[[ $("$KIND" version) == 'kind v0.33.0'* ]] || fail 'Install pinned kind using install-tools.sh.'
[[ $("$KUBECTL" version --client -o json | python3 -c 'import json,sys; print(json.load(sys.stdin)["clientVersion"]["gitVersion"])') == v1.37.0 ]] || fail 'Install pinned kubectl using install-tools.sh.'
owned() {
  [[ -f $STATE_DIR/owned-node-id && -s $KUBECONFIG_FILE ]] || fail 'No ownership record and dedicated kubeconfig. Refusing cluster mutation.'
  local recorded actual
  recorded=$(cat "$STATE_DIR/owned-node-id")
  [[ $recorded =~ ^[a-f0-9]{64}$ ]] || fail 'Invalid ownership record.'
  actual=$(docker inspect --format '{{.Id}} {{index .Config.Labels "io.x-k8s.kind.cluster"}}' "$CLUSTER-control-plane")
  [[ $actual == "$recorded $CLUSTER" ]] || fail 'Cluster container identity no longer matches the ownership record.'
  [[ $("$KUBECTL" --kubeconfig "$KUBECONFIG_FILE" config current-context) == "$CONTEXT" ]] || fail 'Unexpected dedicated context.'
  "$KUBECTL" --kubeconfig "$KUBECONFIG_FILE" --context "$CONTEXT" config view --minify -o json | python3 -c '
import json,sys,urllib.parse
c=json.load(sys.stdin); server=urllib.parse.urlparse(c["clusters"][0]["cluster"]["server"])
assert c["clusters"][0]["name"] == "kind-warexis-phase11"
assert server.scheme == "https" and server.hostname == "127.0.0.1"
'
}
case ${1:-} in
  create)
    [[ $# == 1 ]] || fail 'Usage: cluster.sh create'
    docker info >/dev/null
    if "$KIND" get clusters | grep -Fxq "$CLUSTER"; then fail 'Named cluster already exists. Use status; refusing to adopt it.'; fi
    [[ ! -e $STATE_DIR/owned-node-id && ! -e $KUBECONFIG_FILE ]] || fail 'Existing private state needs inspection before creating another cluster.'
    mkdir -p "$STATE_DIR"
    chmod 700 "$STATE_DIR"
    # Capture ownership even if readiness fails after the node was created.
    set +e
    "$KIND" create cluster --name "$CLUSTER" --config "$repo/infrastructure/kubernetes/kind.yaml" --image "$NODE_IMAGE" --kubeconfig "$KUBECONFIG_FILE" --wait 180s
    result=$?
    set -e
    if node=$(docker inspect --format '{{.Id}} {{index .Config.Labels "io.x-k8s.kind.cluster"}}' "$CLUSTER-control-plane" 2>/dev/null); then
      [[ $node =~ ^([a-f0-9]{64})\ $CLUSTER$ ]] || fail 'Unexpected node labels; inspect manually.'
      printf '%s\n' "${BASH_REMATCH[1]}" > "$STATE_DIR/owned-node-id"
    fi
    [[ ! -f $KUBECONFIG_FILE ]] || chmod 600 "$KUBECONFIG_FILE"
    (( result == 0 )) || fail 'Cluster creation did not become ready; inspect the named node and private state.'
    owned
    "$KUBECTL" --kubeconfig "$KUBECONFIG_FILE" --context "$CONTEXT" wait --for=condition=Ready node --all --timeout=180s
    ;;
  status)
    [[ $# == 1 ]] || fail 'Usage: cluster.sh status'
    owned
    "$KUBECTL" --kubeconfig "$KUBECONFIG_FILE" --context "$CONTEXT" get nodes -o wide
    "$KUBECTL" --kubeconfig "$KUBECONFIG_FILE" --context "$CONTEXT" get pods -n warexis -o wide
    ;;
  delete)
    [[ $# == 3 && $2 == --confirm && $3 == "$CLUSTER" ]] || fail 'Usage: cluster.sh delete --confirm warexis-phase11'
    owned
    "$KIND" delete cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG_FILE"
    # Only these test-owned records are removed; no volume/image/system pruning.
    rm -f -- "$STATE_DIR/owned-node-id" "$KUBECONFIG_FILE"
    ;;
  *) fail 'Usage: cluster.sh {create|status|delete --confirm warexis-phase11}' ;;
esac
