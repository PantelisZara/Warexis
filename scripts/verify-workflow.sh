#!/usr/bin/env bash
set -euo pipefail
repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
for service in order-service inventory-service fulfillment-service; do
  (cd "$repository_root/services/$service" && ./mvnw --batch-mode --no-transfer-progress -DskipTests package)
done
cd "$repository_root"
services/order-service/mvnw --batch-mode --no-transfer-progress -f tests/workflow/pom.xml verify "$@"
