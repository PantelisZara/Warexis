# Kafka workflow verification

This test-only Maven module runs all three packaged service JARs as separate Java 21
processes, one disposable Apache Kafka 4.2.2 broker, and three independent disposable
PostgreSQL 18.6 databases. HTTP requests drive the workflow; Kafka clients observe
DLTs and replay events; SQL observes durable invariants and performs explicit
recovery only inside the test database. It does not use the local Compose stack.

From the repository root with Java 21 and Docker Engine available:

```sh
scripts/verify-workflow.sh
```

The script packages all three services without repeating their unit suites, then runs
this module's verification. Run the full service suites separately. Maven and
Testcontainers dependencies use the same Spring Boot 4.1.1 dependency management
as the services. No production service depends on this module.

For already packaged JARs, including development in separate checkouts:

```sh
services/order-service/mvnw --batch-mode --no-transfer-progress \
  -f tests/workflow/pom.xml \
  -Dorder.jar=/path/to/order-service.jar \
  -Dinventory.jar=/path/to/inventory-service.jar \
  -Dfulfillment.jar=/path/to/fulfillment-service.jar verify
```

Default paths point to each service's `target` JAR in this checkout. Diagnostics
are kept under `tests/workflow/target/order.log` and `inventory.log` and `fulfillment.log`; Maven reports
are under `tests/workflow/target/surefire-reports`.

Coverage includes confirmed multi-line/repeated-SKU orders, all-or-nothing
insufficient/unknown stock rejection, concurrent competing orders, duplicate
records in both directions, and malformed/version/key/UTF-8/trailing-token/
duplicate-property poison records. DLT checks select the current record using source topic, partition, offset, and
consumer group, including when historical records have identical key and bytes.
They preserve bytes, key, source partition and source identity/group headers, confirm source offset progress,
and verify no domain mutation. The suite preserves the original 28 Phase 5 cases and extends the consumer
directions to Fulfillment for these recovery boundaries:

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
identity/source/retry fields and reject the unsafe poison marker in any service log.

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

## Phase 6 workflow extensions

The suite runs 47 packaged-service cases. Its new cases cover full original confirmation snapshots, independent groups and validated unsubscribed events, concurrent identical manual dispatch, immutable shipment origin, duplicate confirmation and same-order aliases after dispatch/restart, conflicting confirmation and wrong-stream Inventory trigger DLT, seven malformed-confirmation vectors, pending OrderConfirmed and FulfillmentShipped outbox JVM recovery, global reference ownership, Fulfillment broker exhaustion/explicit recovery, four-attempt technical rollback/DLT/recovery, and atomic ship/outbox rollback. It also applies the existing post-business-commit checkpoint failure, database outage, and paused-partition/DLT restart matrix to Fulfillment.

A successful order now owns one OrderPlaced and one OrderConfirmed outbox fact; rejection owns only OrderPlaced. Existing fault injections and stored-envelope assertions target the intended event type instead of relying on a single arbitrary row. Original stock, decision, no-mutation, retry-budget, source-offset and recovery assertions remain in place. New poison checks also compare Fulfillment plans, processed confirmations and shipment events after independently waiting for the prior consumer workload to complete.

Run individual full service test/verify lifecycles first and supply their rebuilt current binaries when using separate worktrees. Local verification of the three-service workflow does not imply hosted CI execution; hosted results require an actual hosted run.
