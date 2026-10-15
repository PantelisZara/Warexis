# Inventory Service

Java 21 / Spring Boot stock API backed by its own PostgreSQL database. Flyway V1 owns `stock_items`; V2 owns reservation decisions, processed-event
identities, and outgoing events; Hibernate validates the schema and never generates it.

This is a Bearer-authenticated local-development API. Kafka adapters are opt-in. Reservations
are an internal application use case; no reservation HTTP endpoint is exposed.

## Run locally

Follow the repository's [local infrastructure and host-service recipes](../../infrastructure/local/README.md).
Inventory uses HTTP port 8082 in those recipes. The application requires
`INVENTORY_DB_URL`, `INVENTORY_DB_USERNAME`, and `INVENTORY_DB_PASSWORD`;
It also requires `WAREXIS_OAUTH2_ISSUER_URI` and `WAREXIS_OAUTH2_AUDIENCE`; use
`http://127.0.0.1:18080/realms/warexis` and `warexis-api` for the local identity.
Compose does not export these variables to the host Java process. Use only local example
values for development, never real credentials in committed files.

## HTTP security

Business requests require a valid Bearer access token. Read endpoints permit
`WAREXIS_VIEWER` or `WAREXIS_OPERATOR`; mutations require `WAREXIS_OPERATOR`.
Missing/invalid authentication returns 401; insufficient roles return 403.
Only `GET /actuator/health` is anonymous, without components or sensitive details.
See the [complete authorization matrix](../../docs/architecture/security.md) and
[local Keycloak/token guide](../../infrastructure/auth/README.md).

The curl examples assume an authorized short-lived token already supplied by a
trusted local OIDC client in `WAREXIS_ACCESS_TOKEN`. Keep it in shell memory;
do not save it in `.env`, commit it or log it. The safe Python example in the
identity guide keeps its token in memory and does not export this shell variable.

## Stock API

| Method | Path | Body | Success |
| --- | --- | --- | --- |
| POST | `/api/stock-items` | `{"sku":"SKU-001","onHand":12}` | 201 and `Location: /api/stock-items/SKU-001` |
| GET | `/api/stock-items/{sku}` | none | 200 |
| PUT | `/api/stock-items/{sku}` | `{"onHand":8}` | 200; adjusts total stock without creating missing stock |

All successes return `id` (application-generated UUID), `sku`, `onHand`,
`reserved`, and derived `available = onHand - reserved`. Updating stock preserves
its ID and SKU. New stock starts with `reserved = 0`; there is no reservation
mutation endpoint and `reserved` is not an accepted request field.

SKUs are case-sensitive ASCII strings matching `[A-Za-z0-9][A-Za-z0-9._-]{0,63}`.
They are rejected rather than trimmed or normalized. Quantities must be non-null,
nonnegative JSON integers within the signed 32-bit range. Numeric strings,
fractional numbers, and unknown request fields are rejected.

Errors use `application/problem+json`: invalid or malformed requests return 400,
missing stock returns 404, and duplicate SKUs or stock below the reserved quantity
return 409. Database constraints enforce `0 <= reserved <= onHand` and unique
SKUs. Updates acquire a PostgreSQL row lock inside the application transaction
and validate the latest reserved quantity before committing; a rejected update
leaves existing quantities unchanged.

With the local service running:

```sh
curl -i -H "Authorization: Bearer ${WAREXIS_ACCESS_TOKEN:?Provide an authorized local token}" -H 'Content-Type: application/json' \
  -d '{"sku":"SKU-001","onHand":12}' http://127.0.0.1:8082/api/stock-items
curl -i -H "Authorization: Bearer ${WAREXIS_ACCESS_TOKEN:?Provide an authorized local token}" http://127.0.0.1:8082/api/stock-items/SKU-001
curl -i -X PUT -H "Authorization: Bearer ${WAREXIS_ACCESS_TOKEN:?Provide an authorized local token}" -H 'Content-Type: application/json' \
  -d '{"onHand":8}' http://127.0.0.1:8082/api/stock-items/SKU-001
```

## Internal inventory reservations

`InventoryReservationService.process(OrderPlaced)` accepts a validated event ID,
order ID, and 1 to 100 original lines (`orderLineId`, case-sensitive `sku`, positive
32-bit `quantity`). Order line IDs must be unique. The Kafka adapter validates
the envelope/type/version before invoking this business use case.

The transaction locks event identity and then order identity with PostgreSQL
transaction advisory locks in separate integer namespaces. Hash collisions only
reduce concurrency. Database primary/unique keys also enforce durable event
idempotency and one reservation decision per order. It aggregates repeated SKU
demand in 64-bit integers, locks existing stock rows in case-sensitive ASCII order
(matching PostgreSQL C collation), and checks every SKU before changing any stock.
Unknown SKUs take precedence over insufficient stock. Rejection reserves nothing;
success reserves every line atomically. Existing stock updates use the same row
locks and cannot lower on-hand stock below reserved quantities.

One transaction covers stock updates, immutable `inventory_reservations`,
`processed_order_events`, and an `outbox_events` record. The complete version 1
JSON envelope is stored with metadata and nullable `published_at`; an unpublished
partial index supports the relay. Reserved events omit `reasonCode`;
rejected events include `UNKNOWN_SKU` or `INSUFFICIENT_STOCK`. Both preserve the
original ordered lines, use reservation ID as aggregate ID, order ID as correlation
ID, and the first accepted input event ID as causation ID. See the
[event contracts](../../docs/architecture/event-contracts.md).

An exact duplicate event returns the durable decision without additional writes.
A new event ID for the same order and identical ordered lines records the new
processed identity and returns the original decision without reserving again or
creating another outgoing event. The decision remains final even if stock later
changes. Reusing an event ID for a different order or payload, or an order ID for
different ordered lines, raises `ConflictingOrderPlacedException` and rolls back.
This is unprocessable conflicting input for quarantine handling, not a business
rejection. The Kafka listener validates the wire contract before invoking this
internal application use case.

## Kafka workflow and recovery

Messaging defaults to disabled. Set `WAREXIS_MESSAGING_ENABLED=true` and
`KAFKA_BOOTSTRAP_SERVERS` to enable the listener and scheduled outbox relay.
The adapters create the three-partition local source/output topics and Inventory
DLT, using replication factor one. Inventory consumes `warexis.order.events.v1`
as `warexis.inventory.order-events.v1` and publishes committed decisions to
`warexis.inventory.events.v1`; every workflow record key is the canonical order
UUID. The transport uses raw bytes with strict UTF-8/JSON validation, so malformed
UTF-8, duplicate JSON fields, trailing JSON, unsupported types/versions, bad keys,
and conflicting immutable identities cannot mutate stock. Compatible optional
fields are accepted. A fully validated `OrderConfirmed` is a recognized, unsubscribed
Order fact: the adapter persists COMPLETE and acknowledges it without touching
stock, reservations, processed business events or outgoing Inventory events.
Malformed confirmations still enter the Inventory DLT. Compatibility must be
deployed before Order enables confirmation publication. Business rejection commits normally and is acknowledged;
its single successful processing claim consumes no additional technical retries.

The listener acknowledges only after the reservation transaction commits and
its durable delivery checkpoint is recorded. Transient processing failures get
four total attempts, with 1/2/4-second delays. Invalid contracts/conflicts go
directly to `warexis.order.events.v1.inventory.dlt`. DLT publication retains the
original key/value bytes and source partition, with safe `warexis-*` source,
group, failure classification, attempt, and timestamp headers. Source offsets
advance only after the DLT broker acknowledgment and durable checkpoint. DLT
publication gets four total attempts before its source partition is paused.

V3 `consumer_delivery_state` stores budgets and PROCESS/DLT/COMPLETE/PAUSED state
by consumer group/topic/partition/offset. Attempts are recorded before work;
crashes consume their recorded attempt instead of resetting the bound. PAUSED
records restore their offset and partition pause after reassignment/restart.
Delivery-metadata failures immediately seek/pause without another business
attempt or source acknowledgment; healthy partitions may continue. There is no
automatic DLT consumer or redrive loop.

The relay processes only the earliest unpublished row, waits for broker
acknowledgment, and then marks publication. A PostgreSQL session advisory lock
(namespace 90/key 1) spans publication/checkpointing, allowing one active relay
per Inventory database without leases. Acknowledgment followed by database
failure may publish the same stored event again; consumers must be idempotent.
V3 adds `publication_attempts`/`next_attempt_at` to the outbox and singleton
`outbox_relay_control`. After five total attempts (1/2/4/8-second retry delays),
or immediately for an invalid stored contract, the entire relay durably pauses.
It keeps all unpublished rows and cannot skip the failed row. Metadata failures
stop the current poll before further sends. Producer `acks=all`, zero internal
retries, and finite timeouts keep each recorded attempt bounded.

Recovery is an explicit operator action after remediation. Stop the relevant
adapter before updating retry state, inspect the affected record and preserve
its original identities/bytes, then restart the listener/relay. Reset only the
chosen source row to PROCESS (or DLT when only quarantine delivery failed), its
relevant attempt count and `next_attempt_at`; clear its failure code. For relay
recovery, reset the blocked head row's publication budget/due time and clear the
singleton relay pause. These actions intentionally grant a fresh bounded budget;
a normal application restart grants none. No automatic cleanup or retention
window is implemented. Align broker retention, replay, and durable dedupe
retention before introducing deletion.

`warexis.messaging.retry-base-delay` defaults to `1s` and may be reduced for
integration tests; `warexis.messaging.poll-interval` defaults to `250ms`.
The stock HTTP API remains unchanged when messaging is disabled.

## Verify

Run from this directory with Java 21 and Docker available:

```sh
./mvnw --batch-mode --no-transfer-progress test
./mvnw --batch-mode --no-transfer-progress verify
```

Tests run domain checks and HTTP/persistence integration against disposable
PostgreSQL 18.6 Testcontainers, including schema constraints, atomic reservation/outbox rollback, idempotency,
concurrent duplicate/order races, oversell prevention, and deterministic row-lock
behavior. They do not use the persistent local-development database.

Phase 5 hardening repeatedly synchronizes 36 orders competing for ten units,
30 orders with overlapping and reversed case-sensitive SKU inputs, and 24
concurrent copies or same-order event aliases. Database assertions reconcile
reserved stock with committed successful reservation lines and verify one
decision/outbox per order. Test-only PostgreSQL triggers fail the second stock
update or reservation, processed-event, and outbox inserts; every boundary
rolls back the complete local transaction and the same input can then recover.
These tests prove local atomicity and duplicate tolerance, not distributed
exactly-once delivery.

Failure diagnostics contain validated event/type/order identities, source
coordinates, retry counts, state and classification. Poison identities are
reported as unavailable, with source coordinates identifying the record. Raw
keys, payloads and exception messages are not logged. The outbox partial
`(occurred_at, event_id)` unpublished index matches its head polling order;
no additional index or scaling mechanism is introduced.

## Operational metrics

`GET /actuator/prometheus` requires a signed, valid API JWT with the exact realm
role `WAREXIS_METRICS`. Viewer/operator roles alone cannot scrape it, and the
metrics role alone cannot use business routes. Only `health` and `prometheus` are
exposed; health remains anonymous without details. Bind development ports to
loopback and provide collector credentials outside the repository.

Boot supplies JVM, process, HTTP request latency histograms and Hikari meters.
The fixed `service="inventory"` tag identifies this service. Custom counters use
only bounded `result`/`stage` labels; no business identifier or exception text is a
metric label. They describe process-local observations and reset on restart:

- `warexis_outbox_publications_total{result="published|failed"}` records actual
  sends. Published requires broker acknowledgment and durable publication
  metadata; an acknowledged send whose metadata update fails records failed.
- `warexis_outbox_retries_total` records sends whose durable prior attempt count
  was positive, excluding waiting, paused and idle polls.
- `warexis_messaging_deliveries_total{result="processed|duplicate|ignored|dlt"}`
  records a durably completed delivery after successful source acknowledgment.
  Duplicate includes equivalent business replays and already-complete source
  deliveries; ignored means a validated event to which Inventory does not
  subscribe. A business rejection is processed successfully. A committed business
  effect followed by failed completion/acknowledgment is not counted processed.
- `warexis_messaging_failures_total{stage="process|dlt"}` records actual failed
  processing/send attempts, including malformed contract parsing and failed
  durable DLT completion after broker acknowledgment. Diagnostic validation does
  not increment it. `warexis_messaging_retries_total{stage="process|dlt"}` counts
  actual attempts with a positive durable prior attempt count.
- `warexis_messaging_pauses_total` records successful listener pause requests.
  Assignment/rebalance restoration pauses are excluded. These are listener
  actions, not unique partitions or the current durable paused-partition count.

The outbox sampler uses a dedicated daemon thread, independent of business
scheduling and scrape threads, at a 15-second fixed delay. One SQL snapshot reads
at most the first 10,001 unpublished rows through the existing partial ordered
index, with a two-second SQL execution timeout. Connection acquisition is subject
to the configured datasource timeout and never blocks a scrape or business
scheduler. `warexis_outbox_pending` is capped at 10,001 (meaning at least that
many). `warexis_outbox_oldest_age_seconds` uses database time, excludes published
rows and clamps future timestamps to zero. Empty queues report zero. Query
failure or a snapshot at least 30 seconds old reports NaN for both data gauges and
zero for `warexis_outbox_snapshot_available`; healthy samples report availability
one. Age is a sampled value, not continuously extrapolated. Unavailable data must
not be rendered as a healthy zero. Disabling meters does not change delivery,
transaction, retry, acknowledgment or idempotency behavior.
