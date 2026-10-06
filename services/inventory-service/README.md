# Inventory Service

Java 21 / Spring Boot stock API backed by its own PostgreSQL database. Flyway V1
owns `stock_items`; Hibernate validates the schema and never generates it.

This is an unauthenticated local-development API. Authentication, reservations,
Kafka, outbox, and idempotent consumers are outside this implementation.

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

## Verify

Run from this directory with Java 21 and Docker available:

```sh
./mvnw --batch-mode --no-transfer-progress test
./mvnw --batch-mode --no-transfer-progress verify
```

Tests run domain checks and HTTP/persistence integration against disposable
PostgreSQL 18.6 Testcontainers, including schema constraints and concurrent
row-lock behavior. They do not use the persistent local-development database.
