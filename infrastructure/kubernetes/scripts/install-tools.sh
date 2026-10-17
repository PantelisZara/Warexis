#!/usr/bin/env bash
# Install only version-pinned, SHA256-verified tools into a user-owned directory.
set -euo pipefail
umask 077
KIND_VERSION=v0.33.0
KUBECTL_VERSION=v1.37.0
TOOLS_DIR=${WAREXIS_K8S_TOOLS_DIR:-${XDG_DATA_HOME:-$HOME/.local/share}/warexis/kubernetes/bin}
[[ $# == 0 ]] || { echo 'Usage: install-tools.sh (WAREXIS_K8S_TOOLS_DIR optional)' >&2; exit 2; }
[[ $(uname -s) == Linux ]] || { echo 'This installer supports Linux only.' >&2; exit 1; }
case $(uname -m) in x86_64) arch=amd64 ;; aarch64|arm64) arch=arm64 ;; *) echo 'Unsupported architecture.' >&2; exit 1 ;; esac
repo=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
TOOLS_DIR=$(realpath -m "$TOOLS_DIR")
case "$TOOLS_DIR/" in "$repo/"*) echo 'Tools must be outside the repository.' >&2; exit 1 ;; esac
[[ $TOOLS_DIR != /usr/* && $TOOLS_DIR != /bin && $TOOLS_DIR != /sbin && $TOOLS_DIR != /opt/* ]] || { echo 'Choose a user-owned tools directory.' >&2; exit 1; }
mkdir -p "$TOOLS_DIR"
tmp=$(mktemp -d)
trap 'rm -rf -- "$tmp"' EXIT
fetch() { curl --fail --show-error --silent --location --proto '=https' --tlsv1.2 "$1" -o "$2"; }
verify() {
  local checksum
  checksum=$(awk 'NR==1 {print $1}' "$2")
  [[ $checksum =~ ^[a-fA-F0-9]{64}$ ]] || { echo 'Invalid published checksum.' >&2; exit 1; }
  printf '%s  %s\n' "$checksum" "$1" | sha256sum --check --status
}
fetch "https://github.com/kubernetes-sigs/kind/releases/download/$KIND_VERSION/kind-linux-$arch" "$tmp/kind"
fetch "https://github.com/kubernetes-sigs/kind/releases/download/$KIND_VERSION/kind-linux-$arch.sha256sum" "$tmp/kind.sha256"
verify "$tmp/kind" "$tmp/kind.sha256"
fetch "https://dl.k8s.io/release/$KUBECTL_VERSION/bin/linux/$arch/kubectl" "$tmp/kubectl"
fetch "https://dl.k8s.io/release/$KUBECTL_VERSION/bin/linux/$arch/kubectl.sha256" "$tmp/kubectl.sha256"
verify "$tmp/kubectl" "$tmp/kubectl.sha256"
chmod 700 "$tmp/kind" "$tmp/kubectl"
[[ $("$tmp/kind" version) == "kind $KIND_VERSION"* ]]
[[ $("$tmp/kubectl" version --client -o json | python3 -c 'import json,sys; print(json.load(sys.stdin)["clientVersion"]["gitVersion"])') == "$KUBECTL_VERSION" ]]
install -m 700 "$tmp/kind" "$TOOLS_DIR/kind"
install -m 700 "$tmp/kubectl" "$TOOLS_DIR/kubectl"
printf 'Verified kind %s and kubectl %s installed into the private tools directory.\n' "$KIND_VERSION" "$KUBECTL_VERSION"
