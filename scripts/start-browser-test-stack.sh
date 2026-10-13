#!/usr/bin/env bash
set -euo pipefail

# Foreground fixture only; this script does not run or install any browser driver.
fixture_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
source_root="${1:-$fixture_root}"
runtime_file="${2:-${XDG_STATE_HOME:-$HOME/.local/state}/warexis-browser-fixture/runtime.json}"
realm_file="${3:-$source_root/infrastructure/auth/warexis-realm.json}"
if [[ $# -gt 3 ]]; then
  echo 'Usage: start-browser-test-stack.sh [integrated-source-root] [private-runtime-json] [realm-json]' >&2
  exit 2
fi
exec "$fixture_root/services/order-service/mvnw" --batch-mode --no-transfer-progress \
  -f "$fixture_root/tests/browser-fixture/pom.xml" test \
  -Dtest=BrowserStackSessionTest -Dfixture.start=true \
  "-Dfixture.repository=$source_root" "-Dfixture.realm=$realm_file" "-Dfixture.runtime=$runtime_file"
