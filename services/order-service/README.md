# Warexis Order Service

Java 21 / Spring Boot service owning order persistence and its HTTP API. Flyway
creates the PostgreSQL schema; Hibernate validates it. Order creation persists an
aggregate atomically and starts in `PENDING_INVENTORY`.

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

The API is intended for local development at this stage. Authentication,
authorization, Kafka publication/consumption, outbox, inventory reservations and
status transitions are not implemented. `CONFIRMED` and `REJECTED` are reserved
domain statuses for the later asynchronous workflow. No inventory database is
accessed. Minimum line count is enforced by the aggregate/application boundary;
PostgreSQL independently enforces line values, positions, uniqueness and foreign
keys.

## Verify

Docker must be available for real PostgreSQL 18.6 Testcontainers tests:

```sh
./mvnw --batch-mode --no-transfer-progress -Dtest=OrderTests test
./mvnw --batch-mode --no-transfer-progress test
./mvnw --batch-mode --no-transfer-progress verify
```
