# Initial conceptual event contracts

These contracts are architectural design for the planned messaging milestone. They are not implemented message classes, registered schemas, or finalized wire contracts. Producers and consumers will need contract validation and compatibility tests when messaging is implemented. No shared Java event classes are defined here.

## Common envelope

| Field | Conceptual meaning |
| --- | --- |
| `eventId` | Globally unique identifier for one event; unchanged on publication retries |
| `eventType` | Event name: `OrderPlaced`, `InventoryReserved`, or `InventoryRejected` |
| `eventVersion` | Positive integer contract version; examples use proposed version `1` |
| `occurredAt` | UTC timestamp of the domain occurrence, in ISO 8601 format |
| `aggregateId` | Identifier of the producer's aggregate: order or inventory reservation |
| `correlationId` | Identifier connecting the workflow; initially the `orderId` |
| `causationId` | Triggering event's `eventId` when applicable; omitted for initial REST-triggered creation |
| `payload` | Event-specific business data |

JSON and UUID identifiers are the proposed representation used in the examples; serialization and schema enforcement remain implementation decisions. `orderId` is the Kafka record key for all three events, independently of `aggregateId`.

Payload lines contain `orderLineId`, `sku`, and positive integer `quantity`. They describe the requested lines and preserve their identities across the workflow. Sample identifiers and SKUs are illustrative.

## OrderPlaced

**Producer:** Order Service. **Consumer:** Inventory Service.

Records that an order was accepted into `PENDING_INVENTORY`, not that stock is available. Its order and outbox entry must commit in the same transaction.

**Payload:** `orderId`, `lines`. **Aggregate:** Order; `aggregateId = orderId`.

```json
{
  "eventId": "11111111-1111-4111-8111-111111111111",
  "eventType": "OrderPlaced",
  "eventVersion": 1,
  "occurredAt": "2026-10-06T09:00:00Z",
  "aggregateId": "22222222-2222-4222-8222-222222222222",
  "correlationId": "22222222-2222-4222-8222-222222222222",
  "payload": {
    "orderId": "22222222-2222-4222-8222-222222222222",
    "lines": [
      {
        "orderLineId": "33333333-3333-4333-8333-333333333333",
        "sku": "SKU-001",
        "quantity": 2
      }
    ]
  }
}
```

Inventory must evaluate all lines atomically, account for repeated SKUs, and prevent a replay from reserving stock twice.

## InventoryReserved

**Producer:** Inventory Service. **Consumer:** Order Service.

Records a successful reservation of every requested line. The stock updates, reservation decision, consumer deduplication record, and outcome outbox entry must commit together. Order transitions from `PENDING_INVENTORY` to `CONFIRMED`.

**Payload:** `orderId`, `reservationId`, `lines`. **Aggregate:** InventoryReservation; `aggregateId = reservationId`. `causationId` references the triggering `OrderPlaced` event.

```json
{
  "eventId": "44444444-4444-4444-8444-444444444444",
  "eventType": "InventoryReserved",
  "eventVersion": 1,
  "occurredAt": "2026-10-06T09:00:01Z",
  "aggregateId": "55555555-5555-4555-8555-555555555555",
  "correlationId": "22222222-2222-4222-8222-222222222222",
  "causationId": "11111111-1111-4111-8111-111111111111",
  "payload": {
    "orderId": "22222222-2222-4222-8222-222222222222",
    "reservationId": "55555555-5555-4555-8555-555555555555",
    "lines": [
      {
        "orderLineId": "33333333-3333-4333-8333-333333333333",
        "sku": "SKU-001",
        "quantity": 2
      }
    ]
  }
}
```

## InventoryRejected

**Producer:** Inventory Service. **Consumer:** Order Service.

Records rejection of the entire reservation. No requested stock is reserved. The rejected decision, consumer deduplication record, and outcome outbox entry must commit together. Order transitions from `PENDING_INVENTORY` to `REJECTED`.

**Payload:** `orderId`, `reservationId`, `lines`, `reasonCode`. **Aggregate:** InventoryReservation; `aggregateId = reservationId`. `causationId` references the triggering `OrderPlaced` event. Proposed reason codes are `INSUFFICIENT_STOCK` and `UNKNOWN_SKU`; the final vocabulary remains to be specified.

This example represents an alternative to the successful outcome above, not a second outcome for that order.

```json
{
  "eventId": "66666666-6666-4666-8666-666666666666",
  "eventType": "InventoryRejected",
  "eventVersion": 1,
  "occurredAt": "2026-10-06T09:00:01Z",
  "aggregateId": "55555555-5555-4555-8555-555555555555",
  "correlationId": "22222222-2222-4222-8222-222222222222",
  "causationId": "11111111-1111-4111-8111-111111111111",
  "payload": {
    "orderId": "22222222-2222-4222-8222-222222222222",
    "reservationId": "55555555-5555-4555-8555-555555555555",
    "lines": [
      {
        "orderLineId": "33333333-3333-4333-8333-333333333333",
        "sku": "SKU-001",
        "quantity": 2
      }
    ],
    "reasonCode": "INSUFFICIENT_STOCK"
  }
}
```

## Processing and evolution rules

- Inventory produces one business outcome per order in the initial workflow. Retries may redeliver that outcome; they must not create a contradictory decision.
- Order validates the outcome against the recorded order and allowed lifecycle transition. Duplicate outcomes are harmless; conflicting outcomes require explicit handling.
- Consumer deduplication uses `eventId` together with domain invariants. `correlationId` is for workflow tracing, not event uniqueness.
- Unsupported versions and malformed payloads must not mutate business state. Retry, quarantine, and diagnosis policies remain to be specified.
- Payload changes require compatibility review. Breaking changes require an explicit versioning and rollout strategy rather than silent changes to version `1`.

Kafka topic names and serialization enforcement are intentionally undecided. `OrderConfirmed` is a future Fulfillment integration event and has no initial contract here.
