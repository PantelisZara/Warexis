# ADR 0002: Asynchronous Kafka communication

Status: Accepted architectural direction; messaging workflow implementation is a later milestone.

## Context

Inventory processing can complete independently of the request that creates an order. A synchronous service chain would tie order creation to Inventory availability and obscure the pending business state.

## Decision

Use Kafka to carry `OrderPlaced`, `InventoryReserved`, and `InventoryRejected`. Order creation records `PENDING_INVENTORY`; an inventory outcome later changes it to `CONFIRMED` or `REJECTED`.

Use REST for client-facing commands and queries. Use asynchronous events for the initial cross-service workflow. Use `orderId` as the Kafka record key for these workflow events and assume at-least-once delivery.

## Rationale

Asynchronous communication represents the business process explicitly and lets each service process messages independently. A stable order key supports related records being routed to the same partition within a topic.

## Consequences and trade-offs

- Clients must handle a pending order and query its later status.
- Duplicates, delays, and processing failures are normal conditions to test.
- Kafka adds broker operation and consumer-management requirements.
- Ordering applies within a topic partition, not across topics. The key alone does not guarantee producer publication or consumer processing order.
- Local Kafka infrastructure is available: Apache Kafka 4.2.2 in a single-node KRaft development setup. Producers, consumers, event publication, and event processing remain planned; the business workflow is not operational.
- Capability topic topology, partitioning, groups, bounded retries, and DLT recovery are specified in [ADR 0004](0004-message-topology-and-delivery-semantics.md). Retention must align with the supported replay and cleanup window before enabling deletion.
