#!/usr/bin/env bash
# Small explicit build contexts exclude .env, private credentials and host caches.
set -euo pipefail
umask 077
repo=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
TOOLS_DIR=${WAREXIS_K8S_TOOLS_DIR:-${XDG_DATA_HOME:-$HOME/.local/share}/warexis/kubernetes/bin}
KIND=$TOOLS_DIR/kind
CLUSTER=warexis-phase11
STATE_DIR=$(realpath -m "${WAREXIS_K8S_STATE_DIR:-${XDG_STATE_HOME:-$HOME/.local/state}/warexis-supervisor/phase11/kind}")
tag=p11-14fa97e6
artifacts=$repo
package=false
command=${1:-}
[[ $# == 0 ]] || shift
fail() { echo "$*" >&2; exit 1; }
while [[ $# -gt 0 ]]; do
  case $1 in
    --tag) [[ $# -gt 1 ]] || fail 'Missing image tag.'; tag=$2; shift 2 ;;
    --artifacts) [[ $# -gt 1 ]] || fail 'Missing artifact directory.'; artifacts=$(realpath "$2"); shift 2 ;;
    --package) package=true; shift ;;
    *) fail "Unknown option: $1" ;;
  esac
done
[[ $tag =~ ^p11-14fa97e6(-[a-zA-Z0-9_.-]+)?$ && ${#tag} -le 100 ]] || fail 'Use a Phase11 baseline tag, optionally with a rollout suffix.'
case $command in build|load) ;; *) fail 'Usage: images.sh {build|load} [--tag p11-14fa97e6[-r2]] [--artifacts CHECKOUT_OR_JAR_DIRECTORY] [--package]' ;; esac
if [[ $command == load ]]; then
  [[ $package == false && $artifacts == "$repo" ]] || fail '--package/--artifacts apply only to build.'
  # Reuse lifecycle ownership/context checks before mutating node image storage.
  "$repo/infrastructure/kubernetes/scripts/cluster.sh" status >/dev/null
  [[ $("$KIND" version) == 'kind v0.33.0'* ]] || fail 'Pinned kind is required.'
  images=(postgres:18.6 apache/kafka:4.2.2 quay.io/keycloak/keycloak:26.8.0)
  for image in "${images[@]}"; do docker image inspect "$image" >/dev/null 2>&1 || docker pull "$image"; done
  for service in order inventory fulfillment frontend; do
    docker image inspect "warexis/$service:$tag" >/dev/null
    images+=("warexis/$service:$tag")
  done
  "$KIND" load docker-image --name "$CLUSTER" "${images[@]}"
  exit
fi
if [[ $package == true ]]; then
  [[ $artifacts == "$repo" ]] || fail '--package builds only the current checkout.'
  for service in order inventory fulfillment; do
    (cd "$repo/services/$service-service"; ./mvnw --batch-mode --no-transfer-progress -DskipTests package)
  done
fi
tmp=$(mktemp -d)
trap 'rm -rf -- "$tmp"' EXIT
for service in order inventory fulfillment; do
  jar=$artifacts/services/$service-service/target/$service-service-0.0.1-SNAPSHOT.jar
  [[ -f $jar ]] || jar=$artifacts/$service-service-0.0.1-SNAPSHOT.jar
  [[ -s $jar ]] || fail "Missing $service executable JAR; supply --artifacts or explicitly --package."
  mkdir -p "$tmp/$service"
  cp "$jar" "$tmp/$service/application.jar"
  sha256sum "$tmp/$service/application.jar"
  args=(--build-arg "WAREXIS_BUILD=$tag")
  [[ -z ${WAREXIS_JAVA_IMAGE:-} ]] || args+=(--build-arg "JAVA_IMAGE=$WAREXIS_JAVA_IMAGE")
  docker build "${args[@]}" --file "$repo/infrastructure/kubernetes/images/backend.Dockerfile" --tag "warexis/$service:$tag" "$tmp/$service"
done
mkdir -p "$tmp/frontend"
# Archive committed frontend sources only. The merged candidate must be committed.
git -C "$repo" archive HEAD frontend | tar -x -C "$tmp/frontend"
cp "$repo/infrastructure/kubernetes/frontend-nginx.conf" "$tmp/frontend/frontend-nginx.conf"
args=(--build-arg "WAREXIS_BUILD=$tag")
[[ -z ${WAREXIS_NODE_IMAGE:-} ]] || args+=(--build-arg "NODE_IMAGE=$WAREXIS_NODE_IMAGE")
[[ -z ${WAREXIS_NGINX_IMAGE:-} ]] || args+=(--build-arg "NGINX_IMAGE=$WAREXIS_NGINX_IMAGE")
docker build "${args[@]}" --file "$repo/infrastructure/kubernetes/images/frontend.Dockerfile" --tag "warexis/frontend:$tag" "$tmp/frontend"
printf 'Built four local images with tag %s. Load them into the dedicated cluster before applying manifests.\n' "$tag"
