# Kafka workflow verification

This test-only Maven module runs both packaged service JARs as separate Java 21
processes, one disposable Apache Kafka 4.2.2 broker, and two independent disposable
PostgreSQL 18.6 databases. HTTP requests drive the workflow; Kafka clients observe
DLTs and replay events; SQL observes durable invariants and performs explicit
recovery only inside the test database. It does not use the local Compose stack.

From the repository root with Java 21 and Docker Engine available:

```sh
scripts/verify-workflow.sh
```

The script packages both services without repeating their unit suites, then runs
this module's verification. Run the full service suites separately. Maven and
Testcontainers dependencies use the same Spring Boot 4.1.1 dependency management
as the services. No production service depends on this module.

For already packaged JARs, including development in separate checkouts:

```sh
services/order-service/mvnw --batch-mode --no-transfer-progress \
  -f tests/workflow/pom.xml \
  -Dorder.jar=/path/to/order-service.jar \
  -Dinventory.jar=/path/to/inventory-service.jar verify
```

Default paths point to each service's `target` JAR in this checkout. Diagnostics
are kept under `tests/workflow/target/order.log` and `inventory.log`; Maven reports
are under `tests/workflow/target/surefire-reports`.

Coverage includes confirmed multi-line/repeated-SKU orders, all-or-nothing
insufficient/unknown stock rejection, concurrent competing orders, duplicate
records in both directions, and malformed/version/key/UTF-8/trailing-token/
duplicate-property poison records. DLT checks preserve bytes, key, source
partition and source identity/group headers, confirm source offset progress,
and verify no domain mutation. A broker outage proves durable publication pause,
unchanged event identity across a service restart, and explicit operator recovery.

Tests provision three partitions for each source/DLT stream and use independent
consumer groups for observation. Every created JVM/container is owned by this
run and cleaned up; existing Compose containers and volumes are untouched. The
suite fails if Docker, a service JAR, or workflow startup is unavailable; it does
not silently skip real-broker checks.
