# Inventory Service

Java 21 / Spring Boot stock API backed by its own PostgreSQL database. Flyway V1 owns `stock_items`; V2 owns reservation decisions, processed-event
identities, and outgoing events; Hibernate validates the schema and never generates it.

This is an unauthenticated local-development API. Authentication and Kafka adapters are outside this implementation. Reservations
are an internal application use case; no reservation HTTP endpoint is exposed.

## Run locally

Follow the repository's [local infrastructure and host-service recipes](../../infrastructure/local/README.md).
Inventory uses HTTP port 8082 in those recipes. The application requires
`INVENTORY_DB_URL`, `INVENTORY_DB_USERNAME`, and `INVENTORY_DB_PASSWORD`;
Compose does not export them to the host Java process. Use only local example
values for development, never real credentials in committed files.

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
curl -i -H 'Content-Type: application/json' \
  -d '{"sku":"SKU-001","onHand":12}' http://127.0.0.1:8082/api/stock-items
curl -i http://127.0.0.1:8082/api/stock-items/SKU-001
curl -i -X PUT -H 'Content-Type: application/json' \
  -d '{"onHand":8}' http://127.0.0.1:8082/api/stock-items/SKU-001
```

## Internal inventory reservations

`InventoryReservationService.process(OrderPlaced)` accepts a validated event ID,
order ID, and 1 to 100 original lines (`orderLineId`, case-sensitive `sku`, positive
32-bit `quantity`). Order line IDs must be unique. A future Kafka adapter must
validate the envelope/type/version before invoking this business use case.

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
partial index supports the future relay. Reserved events omit `reasonCode`;
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
This is unprocessable conflicting input for future quarantine handling, not a
business rejection. No relay, polling scheduler, transport retries, or Kafka
producer/consumer is implemented here.

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
