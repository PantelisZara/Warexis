# Version 1 event contracts

These JSON contracts define the Order, Inventory, and Fulfillment workflow. Order creation, Inventory reservation processing, Order confirmation, and manual Fulfillment dispatch persist these envelopes in local transactional outboxes. Opt-in Kafka publishers/listeners now implement offset handling, durable bounded attempts, and DLT delivery. No shared Java event library or Schema Registry is required; each service owns its representations and contract tests.

## Common envelope

All UUID fields use canonical lowercase UUID strings. Events are strict UTF-8 JSON objects, not serialized database entities. Duplicate properties, trailing JSON, and invalid required types are rejected. Required fields must be present and non-null; `causationId` is omitted for initial REST-triggered creation.

| Field | Version 1 contract |
| --- | --- |
| `eventId` | UUID generated once for one domain event; unchanged on publication retries |
| `eventType` | `OrderPlaced`, `InventoryReserved`, `InventoryRejected`, `OrderConfirmed`, or `FulfillmentShipped` |
| `eventVersion` | JSON integer `1` |
| `occurredAt` | ISO 8601 UTC timestamp with `Z`, set when the domain occurrence is recorded |
| `aggregateId` | Order UUID for Order facts; reservation UUID for inventory outcomes; fulfillment UUID for `FulfillmentShipped` |
| `correlationId` | `payload.orderId`, connecting the complete workflow |
| `causationId` | Triggering `OrderPlaced.eventId` for inventory outcomes; first `InventoryReserved.eventId` for `OrderConfirmed`; omitted for REST-triggered `OrderPlaced` and `FulfillmentShipped` |
| `payload` | Object containing the fields defined for the specific event below |

`orderId` is the Kafka record key for all five events, independently of `aggregateId`. Publish the canonical lowercase UUID string as the UTF-8 key; consumers validate that it matches `payload.orderId`. Outbox publication retains the stored event identity, metadata, and JSON values; it must not create a new event or timestamp for a retry. JSON object property order is not semantic, whereas array order is preserved.

Payload `lines` contains 1–100 original order lines in their original order. Each has a unique `orderLineId` UUID within the order, an exact case-sensitive `sku` matching `[A-Za-z0-9][A-Za-z0-9._-]{0,63}`, and a JSON integer `quantity` in `1..2147483647`. There is no case normalization. Repeated SKUs remain separate lines in events and decisions; Inventory sums their quantities using a wide integer before stock checks. An aggregate demand greater than available stock produces a normal rejection, without integer overflow.

The examples use synthetic identifiers and depict alternative outcomes for the same order.

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

Records a successful reservation of every requested line. The stock updates, reservation decision, consumer deduplication record, and outcome outbox entry must commit together. The Order outcome handler transitions `PENDING_INVENTORY` to `CONFIRMED`.

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

Records rejection of the entire reservation. No requested stock is reserved. The rejected decision, consumer deduplication record, and outcome outbox entry must commit together. The Order outcome handler transitions `PENDING_INVENTORY` to `REJECTED`.

**Payload:** `orderId`, `reservationId`, `lines`, `reasonCode`. **Aggregate:** InventoryReservation; `aggregateId = reservationId`. `causationId` references the triggering `OrderPlaced` event. Version 1 reason codes are `INSUFFICIENT_STOCK` and `UNKNOWN_SKU`. If any requested SKU is absent, `UNKNOWN_SKU` takes precedence; otherwise insufficient availability produces `INSUFFICIENT_STOCK`. These are valid business results, not processing errors.

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

## Processing and duplicate identity

The implemented Inventory application use case accepts the input event UUID, order UUID, and original line list. The Kafka adapter validates the full envelope before calling that use case. Reservation business logic stays in the application service.

- Inventory commits stock changes, the reservation decision, the processed-event record, and exactly one outcome outbox event in the same local transaction. A rejected decision changes no stock.
- Repeating an `eventId` with the same order and ordered line list returns the existing decision, with no additional reservation or outgoing event. Reusing that ID with a different order or line list is a conflict and must not mutate state.
- A new `eventId` for the same order and identical ordered line list records that delivery as processed and reuses the original decision. Its original reservation ID and outcome `causationId` remain unchanged. Changed lines, line identities, or order within the list for the same order are a conflict; one order cannot obtain a contradictory decision.
- `eventId` provides message idempotency; unique `orderId` provides business uniqueness. `correlationId` supports tracing and is not a deduplication key.
- The Order outcome handler validates reservation metadata and lines against its recorded order, transitions only `PENDING_INVENTORY` to the matching terminal status, and persists deduplication with the transition. Duplicate matching outcomes are harmless. Conflicting outcomes or inconsistent identities must be quarantined, without overwriting a terminal decision.

Neither local atomic transactions nor Kafka keys provide distributed exactly-once processing. At-least-once publication and delivery require these durable business and message invariants.

## Topics, subscriptions, and ordering

Use capability streams rather than a topic for each event. See [ADR 0004](adr/0004-message-topology-and-delivery-semantics.md) for the decision and alternatives.

| Source topic | Owner / current event types | Consumer group | Subscribed event types | DLT owned by that consumer |
| --- | --- | --- | --- | --- |
| `warexis.order.events.v1` | Order / `OrderPlaced`, `OrderConfirmed` | `warexis.inventory.order-events.v1` | `OrderPlaced` | `warexis.order.events.v1.inventory.dlt` |
| `warexis.inventory.events.v1` | Inventory / `InventoryReserved`, `InventoryRejected` | `warexis.order.inventory-events.v1` | Both inventory outcomes | `warexis.inventory.events.v1.order.dlt` |
| `warexis.order.events.v1` | Order / `OrderPlaced`, `OrderConfirmed` | `warexis.fulfillment.order-events.v1` | `OrderConfirmed` | `warexis.order.events.v1.fulfillment.dlt` |
| `warexis.fulfillment.events.v1` | Fulfillment / `FulfillmentShipped` | None currently | None | None |

All replicas of one consumer service use its group; independent consumers use distinct groups. Consumers filter by `eventType`. A documented, recognized event type outside a group's subscription can be acknowledged without business processing after envelope validation. Unknown types and unsupported versions are quarantined until their contracts are understood, rather than silently discarded. Future consumers get their own failure destinations; one group's failed processing must not interfere with another group's offsets or recovery.

Enabled services provision three partitions per source topic and three for each corresponding DLT, with a single replica in local development. DLT records retain the source partition number; matching partition counts avoid an invalid destination. The three-partition choice permits concurrent processing and partition-order tests without elaborate topology. Local single-replica infrastructure is not production fault tolerance. Source/DLT topic provisioning uses the service Kafka configuration when messaging is enabled.

Kafka preserves record order only within one topic partition. The `orderId` key keeps related records in that topic partition with a stable partition count, but supplies no global or cross-topic order. A relay must publish in its durable outbox order and wait for acknowledgment before passing a failed record; a listener must finish one record's transaction before advancing the partition. Changing partition counts can move a key between partitions; drain and coordinate a migration rather than assuming ordering survives a live resize. Database locks and uniqueness constraints, not Kafka ordering, prevent overselling across different orders.

## Bounded delivery and failure handling

All three services implement these rules with local durable delivery metadata. See [workflow startup and explicit recovery](kafka-workflow.md). No automatic redrive or event cleanup runs.

### Outbox publication

Publish committed events only. Mark `published_at` only after Kafka confirms publication. A crash after acknowledgment and before that update can republish the same event; consumer idempotency makes the duplicate harmless. Do not claim that a successful client response means Kafka has received the event.

Each service relay gets at most five attempts for a pending publication: the initial attempt plus four retries after 1, 2, 4, and 8 seconds. On exhaustion it pauses the entire service relay, surfaces the failure, and retains the pending event. It must not skip the failing row, delete publication intent, or manufacture a business rejection. An operator explicitly resumes after remediation. Attempt counts, due times, and paused state are persisted so a restart cannot silently reset the bound. Serialization/contract defects pause immediately. An unpublished valid domain event belongs in its outbox, not a consumer DLT.

### Consumer processing

Transient technical failures, such as unavailable PostgreSQL, retry the same record in place: one initial attempt plus three retries after 1, 2, and 4 seconds. The partition does not advance during these attempts; other partitions can proceed. This bounded blocking approach avoids extra retry topics and their per-order reordering. Durable per-record counters preserve the budget across restarts. A metadata outage fails closed without invoking business processing or committing the source offset; operator recovery requires fixing the database and inspecting the retained state.

Malformed JSON, invalid envelopes/payloads, unsupported versions/types/reason codes, inconsistent Kafka keys, and conflicting message or business identities go directly to the consumer's DLT with no futile retries and no business mutation. Exhausted transient failures also go to that DLT. `InventoryRejected` is successful business processing: commit the rejected decision and its outgoing event, then acknowledge normally.

A consumer acknowledges a successfully processed source record only after its business transaction and durable delivery completion commit. For DLT recovery, confirm DLT publication and persist durable delivery completion before committing the source offset. Include the original key and bytes plus source topic, partition, offset, group, failure classification, attempt count, and failure timestamp as transport metadata. Preserve the original event when it is parseable; do not replace its event ID. Avoid credentials, full exception dumps, or environment configuration in failure metadata.

A crash between DLT publication and source acknowledgment can duplicate a DLT record; diagnose/redrive using source topic/partition/offset and consumer group. Historical identical keys and bytes do not identify the current failed source record. If DLT publication itself fails after the same bounded technical retry budget, pause the affected partition, leave its source offset uncommitted, and surface the failure for explicit operator recovery. Do not silently drop the record or run an infinite DLT loop.

DLT messages have no automatic consumer or republish loop. Redrive is explicit after fixing the cause, preserves event identity and key, and uses the normal idempotent handler. Redrive can be later than subsequent records, so domain-state validation must reject contradictions. Source and DLT retention and deduplication/outbox cleanup windows must be configured together before enabling deletion; no event or decision cleanup runs.

## Version evolution

Consumers tolerate unknown optional JSON fields. Adding such fields is compatible only when existing required fields, types, identity rules, and meaning remain unchanged. Do not remove or rename a field, change quantity/SKU semantics, or add new mandatory fields or rejection codes silently to version 1.

A breaking envelope/payload change requires a new `eventVersion` and an explicitly versioned capability stream, with a coordinated producer/consumer rollout and replay plan. A new event type in an existing stream needs a documented contract, subscription/filtering review, and consumer compatibility tests before production publication. Avoid both hidden coupling through shared Java classes and treating arbitrary JSON as a validated contract.

## Confirmation and manual dispatch contracts

These additive V1 contracts extend the reservation workflow. The service suites and packaged three-service workflow verify their implemented transaction, identity, and delivery boundaries.

### OrderConfirmed

Order produces this fact on `warexis.order.events.v1` atomically with its first accepted inventory decision and CONFIRMED transition. Fulfillment subscribes as `warexis.fulfillment.order-events.v1`, with its own `warexis.order.events.v1.fulfillment.dlt`. Inventory fully validates this recognized type, records durable delivery completion and acknowledges it without stock, reservation, processed business event or outgoing event changes. Deploy Inventory compatibility before enabling Order publication.

The V1 envelope uses `aggregateId = correlationId = payload.orderId`, `causationId =` the first accepted `InventoryReserved.eventId`, and payload `{orderId,reservationId,lines}`. Lines retain the existing ordered 1–100-line contract. Rejection, redelivery, matching aliases and conflicting terminal outcomes never produce another confirmation. The outbox causation column matches the stored JSON; event identity/time are assigned only once.

### FulfillmentShipped

Fulfillment produces this fact on `warexis.fulfillment.events.v1` atomically with manual dispatch. No current service subscribes to this output. Its aggregate is `fulfillmentId`, correlation and Kafka key are `orderId`, and `occurredAt` is the server-owned `shippedAt`. The immediate cause is a REST operator command without an event identity, so `causationId` is omitted. The workflow origin is explicit `payload.orderConfirmedEventId`.

Payload: `{fulfillmentId,orderId,reservationId,orderConfirmedEventId,shipmentReference,shippedAt,lines}`. The internal case-sensitive reference matches `[A-Za-z0-9][A-Za-z0-9._-]{0,63}` and belongs to one fulfillment. Repeating the identical ship command returns the original projection/time and emits no additional fact. A changed terminal reference or reference reused by another fulfillment is a conflict with no mutation.

Fulfillment creates one PENDING full-order record only from validated OrderConfirmed. It validates/acknowledges unsubscribed OrderPlaced, and never creates fulfillment from InventoryReserved. Same-event changed known content is poison. A new confirmation event identity for the same order with identical reservation, ordered lines and causal origin records an alias, preserving the first confirmation identity and any SHIPPED state; changed origin/reservation/lines or cross-order reservation reuse is poison. Compatible unknown optional JSON fields do not affect known-contract fingerprints.

Fulfillment owns its database, processed-confirmation ledger, transactional outbox, durable retry/DLT state and relay control. It uses the established bounded budgets and acknowledgment rules above. Order remains CONFIRMED after manual shipping. See [ADR 0005](adr/0005-fulfillment-and-manual-dispatch.md).
