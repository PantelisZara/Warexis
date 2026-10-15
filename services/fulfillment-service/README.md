# Fulfillment Service

Java 21 / Spring Boot 4.1.1 service with its own PostgreSQL database and Flyway schema. Hibernate schema mutation and open-in-view are disabled; the fulfillment application uses explicit JDBC transactions and database constraints. No other service's database is accessed.

A valid `OrderConfirmed` creates one `PENDING` fulfillment preserving every original ordered line, including repeated SKUs. `OrderPlaced` is validated and acknowledged without a business mutation. The original confirmation event and causal inventory outcome remain immutable. Matching same-order aliases reuse the plan, including after dispatch; changed known content, reservation ownership or causal origin is quarantined. Optional compatible JSON fields do not change identity comparisons.

The only transition is `PENDING` to `SHIPPED`. This records a manual operator handoff with an internal reference; it does not buy shipping, generate carrier labels or confirm delivery. Order remains `CONFIRMED`. No carrier, payment, address or inventory deduction is performed.

## Local startup

Use Java 21. Provide `FULFILLMENT_DB_URL`, `FULFILLMENT_DB_USERNAME` and `FULFILLMENT_DB_PASSWORD` for the owned PostgreSQL database before running:

```sh
export WAREXIS_OAUTH2_ISSUER_URI=http://127.0.0.1:18080/realms/warexis
export WAREXIS_OAUTH2_AUDIENCE=warexis-api
./mvnw spring-boot:run
```

HTTP defaults to `127.0.0.1:8083`; `FULFILLMENT_HTTP_PORT` overrides the port. This is a secured local portfolio API; keep it on loopback. Only the health actuator endpoint is exposed, without details.

Messaging is disabled by default. Set `WAREXIS_MESSAGING_ENABLED=true` and `KAFKA_BOOTSTRAP_SERVERS` explicitly to enable the consumer and outbox relay. Inventory must support the additive `OrderConfirmed` type before Order publication is enabled. Topics have three partitions with stable order UUID keys:

| Purpose | Topic / group |
| --- | --- |
| Order source | `warexis.order.events.v1` |
| Independent Fulfillment group | `warexis.fulfillment.order-events.v1` |
| Fulfillment consumer DLT | `warexis.order.events.v1.fulfillment.dlt` |
| Fulfillment output | `warexis.fulfillment.events.v1` |

## HTTP security

Business requests require a valid Bearer access token. Read endpoints permit
`WAREXIS_VIEWER` or `WAREXIS_OPERATOR`; mutations require `WAREXIS_OPERATOR`.
Missing/invalid authentication returns 401; insufficient roles return 403.
Only `GET /actuator/health` is anonymous, without components or sensitive details.
See the [complete authorization matrix](../../docs/architecture/security.md) and
[local Keycloak/token guide](../../infrastructure/auth/README.md).

## Operational metrics

`GET /actuator/prometheus` requires the dedicated `WAREXIS_METRICS` realm role.
Viewer/operator roles alone cannot scrape, and a metrics-only identity has no
business API rights. Only health and Prometheus are exposed. Framework HTTP,
JVM, process and Hikari meters carry the fixed `service=fulfillment` label;
HTTP duration histograms support percentile queries.

Custom process-local counters observe outbox publications (`published` only
after broker acknowledgment and committed metadata, otherwise actual `failed`
attempts), outbox retries, messaging delivery completions (`processed`, known
`duplicate`, valid unsubscribed `ignored`, acknowledged and durably completed
`dlt`), processing/DLT failures and retries, and listener partition pause requests.
The pause counter excludes rebalance restoration and does not represent the
current number of paused partitions. Retry counters exclude backoff polls.
Completion metadata failure does not count success. Counters reset on restart;
they are operational observations, not durable business totals. Custom labels
are limited to the fixed `result` and `stage` values, never business identifiers.
Registry failure or disabling metrics does not change business processing.

Outbox gauges share a cached, indexed PostgreSQL snapshot. The pending count is
capped at 10001 (`10001` means at least that many); oldest age uses database time,
clamped to zero for empty/future events. A dedicated daemon thread refreshes at
most every 15 seconds with a one-second query timeout, scanning at most 10001
pending index entries. Scrapes never wait for the business connection pool.
Initial, failed or over-30-second-old snapshots expose `NaN` count/age and
`warexis_outbox_snapshot_available=0`, rather than a misleading healthy zero.

## Owned HTTP API

- `GET /api/fulfillments/{id}` returns the owned projection, or 404.
- `GET /api/fulfillments/orders/{orderId}` returns the owned projection, or 404.
- `POST /api/fulfillments/{id}/ship` with `{"shipmentReference":"MANUAL-001"}` returns the committed projection.

Reference grammar is `[A-Za-z0-9][A-Za-z0-9._-]{0,63}` and is case-sensitive. An identical repeat preserves the original timestamps and outgoing event. A changed terminal reference or reference already owned by another fulfillment returns 409 without mutation. Missing records return 404; malformed canonical UUIDs, invalid bodies/references, duplicate/unknown body fields and trailing JSON return 400. No public create, reset, list or generic status endpoint exists.

Dispatch commits state, shipment reference, timestamp and exactly one stored `FulfillmentShipped` outbox fact together. The fact omits `causationId` because the immediate trigger is an operator command; `payload.orderConfirmedEventId` retains the first accepted workflow confirmation. Kafka consumers can observe duplicate deliveries of the same stored event identity.

## Failure and restart behavior

Delivery is at least once with idempotent local effects. Source acknowledgment follows the business transaction and durable `COMPLETE` metadata. A DLT acknowledgment follows broker confirmation and durable completion. Invalid bytes and source coordinates are retained in the DLT; raw payloads, shipment references and exception details are not written in service diagnostics.

Processing has four total attempts with 1/2/4-second delays; poison or conflicting input goes directly to DLT. DLT publication has four total attempts, then the source partition is durably `PAUSED`, without acknowledgment. Later records on that partition remain blocked while unrelated partitions progress. Assignment restores paused metadata. Retry counts and due times survive process restart.

The relay persists each attempt before publication, publishes only the oldest pending row, and marks publication only after broker acknowledgment. A dedicated PostgreSQL session lock spans metadata persistence and broker calls across publisher instances. Five total attempts use 1/2/4/8-second delays before global relay pause; later pending rows cannot overtake an exhausted head. A crash after broker acknowledgment can republish the same stored fact.

A successful ship command can leave an unpublished outbox fact during broker failure. Read the owned projection and inspect `outbox_events`, `messaging_relay_control` and `messaging_deliveries` separately. Database failure fails closed. Diagnose source group/topic/partition/offset and failure classifications; stop the affected service, repair infrastructure or metadata, then explicitly restart. Exhausted budgets require deliberate operator repair/reset while stopped. There is no automatic redrive, retry-state deletion or public reset endpoint.

## Verification

```sh
./mvnw --batch-mode --no-transfer-progress test
./mvnw --batch-mode --no-transfer-progress verify
```

The service tests use disposable real PostgreSQL and Kafka containers, strict HTTP/transport validation, rollback injection and repeated concurrent identity/reference races. The repository packaged workflow separately verifies the three executable services and restart/failure boundaries. Hosted CI results require an actual hosted run; local verification does not imply publication.
