#!/usr/bin/env bash
set -euo pipefail
repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
"$repository_root/scripts/verify-workflow.sh" '-Dtest=KafkaWorkflowTests#packagedSecurityMatrix+packagedServicesRejectActuallyExpiredKeycloakToken' "$@"
