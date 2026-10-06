# Version 1 event contracts

These JSON contracts define the initial Order/Inventory workflow. Order creation and Inventory reservation processing persist these envelopes in local transactional outboxes. Kafka publishers, listeners, offset handling, retries, and DLT delivery remain the next implementation milestone. No shared Java event library or Schema Registry is required; each service owns its representations and contract tests.

## Common envelope

All UUID fields use canonical UUID strings. Events are UTF-8 JSON objects, not serialized database entities. Required fields must be present and non-null; `causationId` is omitted for initial REST-triggered creation.

| Field | Version 1 contract |
| --- | --- |
| `eventId` | UUID generated once for one domain event; unchanged on publication retries |
| `eventType` | `OrderPlaced`, `InventoryReserved`, or `InventoryRejected` |
| `eventVersion` | JSON integer `1` |
| `occurredAt` | ISO 8601 UTC timestamp with `Z`, set when the domain occurrence is recorded |
| `aggregateId` | Order UUID for `OrderPlaced`; reservation UUID for either inventory outcome |
| `correlationId` | `payload.orderId`, connecting the complete workflow |
| `causationId` | Triggering `OrderPlaced.eventId` for inventory outcomes; omitted for `OrderPlaced` |
| `payload` | Object containing the fields defined for the specific event below |

`orderId` is the Kafka record key for all three events, independently of `aggregateId`. Publish the canonical lowercase UUID string as the UTF-8 key; consumers validate that it matches `payload.orderId`. Outbox publication retains the stored event identity, metadata, and JSON values; it must not create a new event or timestamp for a retry. JSON object property order is not semantic, whereas array order is preserved.

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

Records a successful reservation of every requested line. The stock updates, reservation decision, consumer deduplication record, and outcome outbox entry must commit together. The future Order outcome handler will transition `PENDING_INVENTORY` to `CONFIRMED`.

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

Records rejection of the entire reservation. No requested stock is reserved. The rejected decision, consumer deduplication record, and outcome outbox entry must commit together. The future Order outcome handler will transition `PENDING_INVENTORY` to `REJECTED`.

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

The implemented Inventory application use case accepts the input event UUID, order UUID, and original line list. The next Kafka adapter must validate the full envelope before calling that use case. It must not move reservation business logic into the listener.

- Inventory commits stock changes, the reservation decision, the processed-event record, and exactly one outcome outbox event in the same local transaction. A rejected decision changes no stock.
- Repeating an `eventId` with the same order and ordered line list returns the existing decision, with no additional reservation or outgoing event. Reusing that ID with a different order or line list is a conflict and must not mutate state.
- A new `eventId` for the same order and identical ordered line list records that delivery as processed and reuses the original decision. Its original reservation ID and outcome `causationId` remain unchanged. Changed lines, line identities, or order within the list for the same order are a conflict; one order cannot obtain a contradictory decision.
- `eventId` provides message idempotency; unique `orderId` provides business uniqueness. `correlationId` supports tracing and is not a deduplication key.
- The future Order outcome handler must validate reservation metadata and lines against its recorded order, transition only `PENDING_INVENTORY` to the matching terminal status, and persist deduplication with the transition. Duplicate matching outcomes are harmless. Conflicting outcomes or inconsistent identities must be quarantined, without overwriting a terminal decision.

Neither local atomic transactions nor Kafka keys provide distributed exactly-once processing. At-least-once publication and delivery require these durable business and message invariants.

## Topics, subscriptions, and ordering

Use capability streams rather than a topic for each event. See [ADR 0004](adr/0004-message-topology-and-delivery-semantics.md) for the decision and alternatives.

| Source topic | Owner / current event types | Consumer group | Subscribed event types | DLT owned by that consumer |
| --- | --- | --- | --- | --- |
| `warexis.order.events.v1` | Order / `OrderPlaced` | `warexis.inventory.order-events.v1` | `OrderPlaced` | `warexis.order.events.v1.inventory.dlt` |
| `warexis.inventory.events.v1` | Inventory / `InventoryReserved`, `InventoryRejected` | `warexis.order.inventory-events.v1` | Both inventory outcomes | `warexis.inventory.events.v1.order.dlt` |

All replicas of one consumer service use its group; independent consumers use distinct groups. Consumers filter by `eventType`. A documented, recognized event type outside a group's subscription can be acknowledged without business processing after envelope validation. Unknown types and unsupported versions are quarantined until their contracts are understood, rather than silently discarded. Future consumers get their own failure destinations; one group's failed processing must not interfere with another group's offsets or recovery.

The next adapter milestone should provision three partitions per source topic and three for each corresponding DLT, with a single replica in local development. DLT records retain the source partition number; matching partition counts avoid an invalid destination. The three-partition choice permits concurrent processing and partition-order tests without elaborate topology. Local single-replica infrastructure is not production fault tolerance. Topic provisioning is not implemented by this documentation change.

Kafka preserves record order only within one topic partition. The `orderId` key keeps related records in that topic partition with a stable partition count, but supplies no global or cross-topic order. A relay must publish in its durable outbox order and wait for acknowledgment before passing a failed record; a listener must finish one record's transaction before advancing the partition. Changing partition counts can move a key between partitions; drain and coordinate a migration rather than assuming ordering survives a live resize. Database locks and uniqueness constraints, not Kafka ordering, prevent overselling across different orders.

## Bounded delivery and failure handling

These rules are requirements for the next adapter milestone; Phase 3 contains no relay, listener, retry state, DLT infrastructure, or operator recovery tool.

### Outbox publication

Publish committed events only. Mark `published_at` only after Kafka confirms publication. A crash after acknowledgment and before that update can republish the same event; consumer idempotency makes the duplicate harmless. Do not claim that a successful client response means Kafka has received the event.

The planned service relay gets at most five attempts for a pending publication: the initial attempt plus four retries after 1, 2, 4, and 8 seconds. On exhaustion it pauses the entire service relay, surfaces the failure, and retains the pending event. It must not skip the failing row, delete publication intent, or manufacture a business rejection. An operator explicitly resumes after remediation. The next milestone must persist attempts and paused state so a restart cannot silently reset the bound; Phase 3 outboxes currently have only minimal publication metadata and do not implement those counters. Serialization/contract defects pause immediately. An unpublished valid domain event belongs in its outbox, not a consumer DLT.

### Consumer processing

Transient technical failures, such as unavailable PostgreSQL, retry the same record in place: one initial attempt plus three retries after 1, 2, and 4 seconds. The partition does not advance during these attempts; other partitions can proceed. This bounded blocking approach avoids extra retry topics and their per-order reordering. The next milestone must ensure the attempt budget survives restart, using durable retry metadata or an equivalent tested recovery design; a restarted listener must not enter an unlimited fresh retry cycle.

Malformed JSON, invalid envelopes/payloads, unsupported versions/types/reason codes, inconsistent Kafka keys, and conflicting message or business identities go directly to the consumer's DLT with no futile retries and no business mutation. Exhausted transient failures also go to that DLT. `InventoryRejected` is successful business processing: commit the rejected decision and its outgoing event, then acknowledge normally.

A consumer acknowledges a successfully processed source record only after its database transaction commits. For DLT recovery, confirm DLT publication before committing the source offset. Include the original key and bytes plus source topic, partition, offset, group, failure classification, attempt count, and failure timestamp as transport metadata. Preserve the original event when it is parseable; do not replace its event ID. Avoid credentials, full exception dumps, or environment configuration in failure metadata.

A crash between DLT publication and source acknowledgment can duplicate a DLT record; diagnose/redrive using source topic/partition/offset. If DLT publication itself fails after the same bounded technical retry budget, pause the affected partition, leave its source offset uncommitted, and surface the failure for explicit operator recovery. Do not silently drop the record or run an infinite DLT loop.

DLT messages have no automatic consumer or republish loop. Redrive is explicit after fixing the cause, preserves event identity and key, and uses the normal idempotent handler. Redrive can be later than subsequent records, so domain-state validation must reject contradictions. Source and DLT retention and deduplication/outbox cleanup windows must be configured together before enabling deletion; Phase 3 performs no event or decision cleanup.

## Version evolution

Consumers tolerate unknown optional JSON fields. Adding such fields is compatible only when existing required fields, types, identity rules, and meaning remain unchanged. Do not remove or rename a field, change quantity/SKU semantics, or add new mandatory fields or rejection codes silently to version 1.

A breaking envelope/payload change requires a new `eventVersion` and an explicitly versioned capability stream, with a coordinated producer/consumer rollout and replay plan. A new event type in an existing stream needs a documented contract, subscription/filtering review, and consumer compatibility tests before production publication. Avoid both hidden coupling through shared Java classes and treating arbitrary JSON as a validated contract.

`OrderConfirmed` remains a future Fulfillment integration event and has no contract or publication in this milestone.
