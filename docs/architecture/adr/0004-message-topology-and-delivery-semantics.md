# ADR 0004: Capability event streams and bounded delivery

Status: Accepted contract and delivery design; Kafka adapters and recovery mechanisms are not implemented yet.

## Context

The first asynchronous workflow has one Order event and two alternative Inventory outcomes. Services own separate databases and use local transactional outboxes. Publication and consumer recovery can duplicate records. We need understandable routing and failure handling before connecting Kafka.

## Decision

Use UTF-8 JSON version 1 envelopes in two service-owned capability streams: `warexis.order.events.v1` and `warexis.inventory.events.v1`. Use `orderId` as the record key for every workflow event and filter with envelope `eventType`. Inventory consumes Order events as `warexis.inventory.order-events.v1`; Order consumes Inventory outcomes as `warexis.order.inventory-events.v1`. Each independent service subscription has its own group and DLT.

Start with three partitions per source and matching three-partition DLTs, retaining the source partition when quarantining. Current DLTs are `warexis.order.events.v1.inventory.dlt` and `warexis.inventory.events.v1.order.dlt`. Local replication remains one; production durability is a later operational decision.

Use at-least-once delivery with durable consumer idempotency and business uniqueness. Commit domain data and outgoing events together; publish outbox records only after commit and mark publication only after broker acknowledgment. Acknowledge consumption after the local processing transaction commits.

Use bounded retries in place rather than retry topics. Transient consumer failures get three retries after the first attempt (1, 2, 4 seconds); poison messages go directly to DLT. Confirm DLT publication before acknowledging the source record. A DLT publication failure pauses its partition after bounded retries. The relay gets four retries (1, 2, 4, 8 seconds) and then pauses the entire service relay, retaining all unpublished intent. Restart must not reset either retry bound; durable retry/pause handling is required in the adapter milestone and is absent from Phase 3. Recovery/redrive is explicit, with original identities retained. Business inventory rejection is successful processing, never a technical retry.

The [event contracts](../event-contracts.md) specify validation, duplicate conflicts, metadata, and recovery in detail. Compatible optional additions retain version 1; breaking changes need a new versioned stream and coordinated rollout. No shared event library or Schema Registry is introduced.

## Alternatives

- **One topic per event type:** Simple subscriptions, but splits one capability's closely related outcomes and multiplies topics, DLTs, and configuration as types grow.
- **One global event topic:** Fewer topics, but weakens service ownership and forces unrelated consumers to share routing and retention concerns.
- **Nonblocking retry topics:** Can free a partition during long failures, but add routing, recovery state, and ordering gaps. Defer until measured throughput requirements justify them.
- **Distributed exactly-once or shared database transactions:** Do not cover independent domain databases and create unnecessary coupling; local transactions plus idempotency fit the workflow.

## Rationale

Two capability streams make ownership visible and allow event types to evolve without creating a topic for every business fact. Minimal JSON remains inspectable and testable within the portfolio scope. In-place bounded retries preserve source-partition processing order until a record is processed or quarantined, and explicit pause/DLT recovery avoids silent loss and infinite retry loops.

## Trade-offs and consequences

- Kafka ordering applies only within one topic partition. Stable keys require stable partitioning; publication and processing order still need enforcement. DLT redrive has no original ordering guarantee.
- Blocking retries delay all orders on the affected partition. Other partitions can continue; domain locking controls stock shared across orders.
- Broker acknowledgment followed by database failure can duplicate publication. Database commit followed by offset failure can duplicate consumption. Both are expected and must be tested.
- Pausing a service relay prevents later events overtaking a failed publication but affects unrelated orders until recovery.
- Consumer-specific DLTs isolate future subscriptions, at the cost of one extra topic per consuming service. DLTs need diagnosis and explicit recovery rather than automatic loops.
- Durable retry budgets, paused state, cleanup windows, broker acknowledgment settings, and real Kafka failure tests are implementation obligations for the next milestone. The local outboxes alone do not provide an operational Kafka workflow.
