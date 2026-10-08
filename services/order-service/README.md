# Warexis Order Service

Java 21 / Spring Boot service owning order persistence, its HTTP API and Kafka
workflow adapters. Flyway
creates the PostgreSQL schema; Hibernate validates it. Order creation persists an
aggregate and one durable `OrderPlaced` outbox event in the same transaction,
and starts in `PENDING_INVENTORY`.

## Run locally

Start the repository's local PostgreSQL infrastructure using
[the infrastructure guide](../../infrastructure/local/README.md). Export these
variables in the shell that launches this service:

```sh
export ORDER_DB_URL=jdbc:postgresql://127.0.0.1:15432/warexis_order
export ORDER_DB_USERNAME=warexis_order
export SERVER_ADDRESS=127.0.0.1
export SERVER_PORT=8081
read -rsp 'Order database password: ' ORDER_DB_PASSWORD
echo
export ORDER_DB_PASSWORD
./mvnw spring-boot:run
```

Run commands from `services/order-service`. The password must match the configured
`ORDER_POSTGRES_PASSWORD`; Compose's `--env-file` does not export variables to a
separately launched Java process. Application port remains runtime-configurable.

## API

Create an order:

```sh
curl -i http://localhost:8081/api/orders \
  -H 'Content-Type: application/json' \
  -d '{"lines":[{"sku":"SKU-1","quantity":2},{"sku":"SKU-1","quantity":3}]}'
```

`POST /api/orders` returns `201 Created` with `Location: /api/orders/{id}` and:

```json
{
  "id": "a4ae795b-2cae-457f-91cf-586efb227d8a",
  "status": "PENDING_INVENTORY",
  "createdAt": "2026-10-06T10:00:00.123456Z",
  "updatedAt": "2026-10-06T10:00:00.123456Z",
  "lines": [
    {"id": "7849506b-47cc-4802-97c5-e457d94440cb", "sku": "SKU-1", "quantity": 2},
    {"id": "0c9a5131-de31-46f4-b94a-57c088745f70", "sku": "SKU-1", "quantity": 3}
  ]
}
```

IDs and timestamps in this example are illustrative; use the returned ID to read
the persisted order:

```sh
curl -i http://localhost:8081/api/orders/REPLACE_WITH_RETURNED_UUID
curl -i http://localhost:8081/actuator/health
```

`GET /api/orders/{id}` returns `200` with the same representation, `404` for a
missing order, or `400` for a malformed UUID. Line IDs and input order remain
stable after reload. Creation timestamps use UTC with PostgreSQL microsecond
precision; `updatedAt` initially equals `createdAt`.

Requests require 1–100 non-null lines. SKUs are case-sensitive ASCII strings
matching `[A-Za-z0-9][A-Za-z0-9._-]{0,63}` with no trimming or normalization.
Repeated SKUs remain separate lines. Quantities must be positive JSON integers
between 1 and 2147483647; fractional numbers and numeric strings are rejected.
Invalid input or malformed JSON returns `400` with safe `application/problem+json`
details and creates no partial order. Unsupported HTTP methods return `405` and
unsupported request media types return `415`.

The API is intended for local development at this stage. Authentication and
authorization are not implemented. With messaging enabled, validated Inventory
outcomes transition `PENDING_INVENTORY` to `CONFIRMED` or `REJECTED`. No inventory database is
accessed. Minimum line count is enforced by the aggregate/application boundary;
PostgreSQL independently enforces line values, positions, uniqueness and foreign
keys.

## Transactional outbox

Order creation writes the order, original lines and `outbox_events` row in one
PostgreSQL transaction. Failure of any write rolls back the entire creation.
Each row stores the complete version 1 JSON envelope from
[the event contracts](../../docs/architecture/event-contracts.md), plus event ID,
aggregate ID, event type/version, occurrence time, correlation ID and optional
causation ID as queryable metadata. `OrderPlaced` uses the order creation time
at microsecond precision, `aggregateId = correlationId = orderId`, and omits
`causationId`. Payload lines preserve original identities and order, including
repeated SKUs; no persistence entities are serialized.

The event ID is unique. An additional aggregate/type/version constraint prevents
a second v1 event of either Order-owned type for the same order. PostgreSQL checks envelope
metadata consistency and enforces an aggregate foreign key. `published_at` is
initially null, with a partial `(occurred_at, event_id)` index for polling
of unpublished rows. The relay reuses the persisted envelope and event ID,
since acknowledgment followed by a database failure can duplicate delivery.

## Kafka workflow

Messaging defaults to disabled, keeping HTTP/database-only development available.
To enable the workflow with the repository's local Kafka broker, also export:

```sh
export WAREXIS_MESSAGING_ENABLED=true
export KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19092
```

Order publishes `OrderPlaced` and `OrderConfirmed` to `warexis.order.events.v1` and consumes Inventory
outcomes from `warexis.inventory.events.v1` as group
`warexis.order.inventory-events.v1`. Source topics and the Order consumer DLT
`warexis.inventory.events.v1.order.dlt` have three partitions and local replication
one. Keys are canonical order UUIDs. UTF-8 JSON is validated without coercion;
unknown optional fields are tolerated, while known fields, version, original
ordered line identities, SKUs, quantities and actual `OrderPlaced` causation must
match. The transactional outcome handler row-locks the order and durably records
message identity and one inventory decision per order. A new event ID is a safe
alias only when all known decision metadata, including occurrence timestamp,
reservation ID, causation and ordered lines, matches. Conflicts preserve the
original terminal status and go to DLT, in either direction (`CONFIRMED` followed
by rejection, or `REJECTED` followed by reservation). A structurally valid outcome
for an unknown order also goes directly to DLT, without futile technical retries.
Identical rejected outcomes and matching aliases are idempotent, like successful
reservation outcomes.

The first accepted `InventoryReserved` atomically commits `CONFIRMED`, its
inventory decision, processed input and one `OrderConfirmed` outbox event. Its
causation ID is that first accepted Inventory event ID, stored in both metadata
and JSON; the original ordered lines and reservation ID are preserved. Rejection,
redelivery, matching aliases and conflicts create no second confirmation. Order
remains `CONFIRMED` after a later manual Fulfillment dispatch; this service does
not consume `FulfillmentShipped` or own a shipping status. Enable publication only
with consumers that recognize and validate the additive `OrderConfirmed` type,
including the compatible Inventory consumer.

The relay acquires a dedicated PostgreSQL session advisory lock so only one
replica publishes at a time, takes the oldest unpublished `(occurred_at,event_id)`
row, and waits for broker acknowledgment before marking publication. Each attempt
is durably reserved before sending. Kafka producer retries are disabled; sends
use `acks=all`, one in-flight request and a bounded acknowledgment timeout.
At most five relay calls are allowed, with 1, 2, 4 and 8 second delays; exhaustion
pauses the entire relay in `messaging_relay_control`. Invalid stored events pause
immediately. Failed intent remains unpublished and later rows cannot overtake it.

The listener processes one record per poll, using three concurrent consumers.
A successful local transaction and durable delivery completion precede manual
source acknowledgment. Transient processing failures get four attempts total
with 1, 2 and 4 second delays; valid business rejection succeeds normally. Poison
or exhausted technical failures go to the consumer DLT with original key/value
bytes and source partition. DLT metadata contains bounded source identity, group,
failure code, processing/DLT attempt counts and UTC failure time; no exception
stack or environment values are included. DLT publication is confirmed before
source acknowledgment. Four failed DLT attempts durably pause that partition.

`messaging_deliveries` stores phase and attempt budgets by topic/partition/offset;
the single fixed Order subscription is implicit in this service-owned database.
Retries and paused state survive process restart. Assignment restores paused
positions, and retry metadata failure seeks/pauses without invoking business
processing. Other partitions can continue. Unexpected container errors stop the
container rather than using a second framework recovery loop. Duplicate source
or DLT delivery remains possible, and business/message idempotency handles it.

Delivery diagnostics include validated event ID/type/order ID when available,
state, attempt count, and source partition/offset. Malformed records use source
coordinates without logging their raw key or payload. Relay diagnostics retain
the stored event/order identity and publication attempt. Local tests verify
competing relay instances cannot overtake an unacknowledged head event; the
session lock is the coordination boundary, rather than an in-memory flag.

`WAREXIS_MESSAGING_RETRY_BASE_DELAY` defaults to `1s` and
`WAREXIS_MESSAGING_POLL_INTERVAL` to `250ms`. Test configurations use shorter
delays; retry base must be positive and at most one second, keeping blocking
retries inside the consumer poll interval. `spring.kafka.listener.auto-startup=false` keeps listeners stopped for
controlled verification; it does not enable messaging or reset durable budgets.

## Explicit recovery

No automatic resume, DLT redrive or deletion is performed. After diagnosis and
remediation, an operator can use the Order database to reset the failed head's
publication budget and resume its relay. Replace the event UUID before running:

```sql
BEGIN;
UPDATE outbox_events SET publication_attempts = 0, next_attempt_at = NULL
WHERE event_id = 'REPLACE_WITH_FAILED_EVENT_UUID' AND published_at IS NULL;
UPDATE messaging_relay_control SET paused = FALSE, failure_classification = NULL WHERE id = 1;
COMMIT;
```

For a DLT-unavailable partition, reset the identified record's DLT budget and
restart the service so its partition is assigned again. Replace partition and
offset placeholders with the diagnosed source identity:

```sql
UPDATE messaging_deliveries SET phase = 'DLT', dlt_attempts = 0, next_attempt_at = NULL
WHERE source_topic = 'warexis.inventory.events.v1'
  AND source_partition = REPLACE_PARTITION AND source_offset = REPLACE_OFFSET AND phase = 'PAUSED';
```

If retry metadata was unavailable, restore the database and explicitly restart
the paused listener/service; persisted attempt counts still apply. To redrive an
already quarantined event, preserve its original key and event identity and use
the normal source subscription after correcting the cause. Do not manufacture a
new business outcome, move source offsets past unfinished records, or expect DLT
redrive to preserve its former ordering. Retention/cleanup remains deferred.

## Verify

Docker must be available for real PostgreSQL 18.6 Testcontainers tests:

```sh
./mvnw --batch-mode --no-transfer-progress -Dtest=OrderTests test
./mvnw --batch-mode --no-transfer-progress test
./mvnw --batch-mode --no-transfer-progress verify
```
