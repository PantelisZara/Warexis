# Kafka workflow verification

This test-only Maven module runs all three packaged service JARs as separate Java 21
processes, one disposable Apache Kafka 4.2.2 broker, three independent disposable
business PostgreSQL 18.6 databases, and actual Keycloak 26.8.0 with a separate
identity PostgreSQL database. Real Keycloak OPERATOR tokens authenticate external
HTTP workflow commands; Kafka events contain no access credentials. HTTP requests drive the workflow; Kafka clients observe
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
  -Dfulfillment.jar=/path/to/fulfillment-service.jar \
  -Dkeycloak.realm=/path/to/infrastructure/auth/warexis-realm.json verify
```

Default paths point to each service's `target` JAR and `infrastructure/auth/warexis-realm.json` in this checkout. Diagnostics
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

The suite preserves 47 packaged workflow cases and adds four real-token security cases (51 total). Its new cases cover full original confirmation snapshots, independent groups and validated unsubscribed events, concurrent identical manual dispatch, immutable shipment origin, duplicate confirmation and same-order aliases after dispatch/restart, conflicting confirmation and wrong-stream Inventory trigger DLT, seven malformed-confirmation vectors, pending OrderConfirmed and FulfillmentShipped outbox JVM recovery, global reference ownership, Fulfillment broker exhaustion/explicit recovery, four-attempt technical rollback/DLT/recovery, and atomic ship/outbox rollback. It also applies the existing post-business-commit checkpoint failure, database outage, and paused-partition/DLT restart matrix to Fulfillment.

A successful order now owns one OrderPlaced and one OrderConfirmed outbox fact; rejection owns only OrderPlaced. Existing fault injections and stored-envelope assertions target the intended event type instead of relying on a single arbitrary row. Original stock, decision, no-mutation, retry-budget, source-offset and recovery assertions remain in place. New poison checks also compare Fulfillment plans, processed confirmations and shipment events after independently waiting for the prior consumer workload to complete.

Run individual full service test/verify lifecycles first and supply their rebuilt current binaries when using separate worktrees. Local verification of the three-service workflow does not imply hosted CI execution; hosted results require an actual hosted run.

## Real Keycloak security gate

`scripts/verify-security.sh` builds current executable JARs and runs only the four
packaged security cases. `scripts/verify-workflow.sh` runs all 51 cases. Neither
subset success nor old binary evidence establishes full workflow success.

The fixture imports the exact repository realm, checks OIDC discovery and JWKS,
and provisions temporary scoped service-account clients through a temporary
bootstrap administrative service account. It uses `client_credentials`, disables
password/browser grants, generates secrets at runtime, and removes bootstrap
after provisioning. Tokens and secrets stay in memory. OPERATOR tokens refresh
before expiry throughout the lengthy restart/recovery run.

Each packaged service rejects missing/malformed credentials, missing or explicitly
wrong audiences, and a separately issued token with the intended API audience.
Real VIEWER reads succeed and commands return 403; OPERATOR reads/commands succeed.
Actual OAuth scopes, client-role arrays and lookalike realm-role arrays cannot
grant Warexis roles. Health remains anonymous without component/details data,
other Actuator/undefined routes are denied, duplicate Authorization headers return
standard 400, and responses do not set authentication cookies. One short-lived
real token is retained beyond expiration plus the framework clock-skew allowance;
all three services reject it. Stored outgoing domain events are inspected for
access credentials. The unchanged workflow cases then prove the authenticated
command → Kafka → Inventory → Kafka → Order → Kafka → Fulfillment chain with the
existing idempotency, DLT, offset, bounded retry and restart invariants.

The verifier owns and cleans up its separate identity/business resources. It does
not use/reset the persistent local Compose identity or business data. Hosted CI
defines three service verification jobs and this complete packaged gate with a
45-minute timeout; no remote identity credentials or secrets are required.

## Operational metrics regression

The packaged security matrix additionally checks anonymous scrape rejection,
viewer/operator scrape denial, metrics-only scrape access, and metrics-only denial
on real business reads and mutations. A final real workload observes durable Order
publication, duplicate Inventory delivery without another reservation, and an
acknowledged Fulfillment DLT outcome. The existing exhausted-DLT scenarios also
assert the observed failed-send and retry counters before restarting the service.
Metric privacy checks reject business identifiers and credential material in the
scrapes. These checks use the packaged services and signed disposable Keycloak
JWTs; they do not require Prometheus or Grafana. Dashboard acceptance is a separate
real monitoring-stack gate.
