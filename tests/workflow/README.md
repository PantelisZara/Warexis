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
and verify no domain mutation. The suite has 28 cases, including both service
directions for these recovery boundaries:

- A committed pending outbox survives forced JVM termination and publishes the
  same stored envelope after restart.
- A consumer whose business transaction committed but whose delivery checkpoint
  failed leaves the source offset uncommitted. After forced termination and
  restart, replay completes without another reservation, decision or outbox.
- An unavailable owned database leaves the observed source offset uncommitted;
  restoring that database and restarting the affected service completes delivery.
- Four real failed DLT sends exhaust the durable budget and pause one partition.
  At least three distinct later order IDs on that partition remain undecided,
  while another partition progresses before and after process restart. Explicit
  recovery releases the blocked record and all later offsets. A further restart
  and a new record on the recovered partition prove terminal DLT state is retained.

A broker outage separately proves durable publication pause, unchanged event
identity across an Order process restart, and explicit operator recovery. SQL
triggers and DLT message-size limits inject failures only into disposable test
resources. Service logs reset once per suite run and append across that run's
restarts, so old diagnostics cannot satisfy assertions. Checks require safe
identity/source/retry fields and reject the unsafe poison marker in either log.

Run the service suites as well: they exercise conflicting outcomes, bounded
processing failures, publication acknowledgment/checkpoint boundaries, repeated
heavy reservation contention, concurrent duplicate event IDs and same-order
aliases, deterministic multi-SKU locking, and every local persistence rollback
boundary. The packaged suite does not replace those checks.

Tests provision three partitions for each source/DLT stream and use independent
consumer groups for observation. Every created JVM/container is owned by this
run and cleaned up; existing Compose containers and volumes are untouched. The
suite fails if Docker, a service JAR, or workflow startup is unavailable; it does
not silently skip real-broker checks.
