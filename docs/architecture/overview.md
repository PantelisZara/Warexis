# Architecture overview

## Status and scope

Order and Inventory service foundations are present with independent configuration and data ownership. Both use Java 21, Spring Boot 4.1.1, Maven, PostgreSQL-ready datasource configuration, Flyway, JPA/Hibernate, and Actuator health endpoints, with executable PostgreSQL Testcontainers integration smoke tests.

Local Docker Compose infrastructure provides separate PostgreSQL 18.6 containers and Apache Kafka 4.2.2 in a single-node KRaft setup, with persistent Docker volumes and health checks. This infrastructure does not yet provide an operational business messaging workflow.

Order creation/read and Inventory stock create/read/update APIs now have independent domain persistence and Flyway business schemas. Inventory stock updates take a database row lock and preserve stock invariants. Order creation also persists an `OrderPlaced` transactional outbox event. Inventory has an internal reservation use case with all-or-nothing stock locking, durable processed-event records, one decision per order, and an outcome outbox. Orders stay `PENDING_INVENTORY` because the cross-service messaging workflow remains planned: Kafka publishers/listeners, delivery recovery, and Order outcome transitions are not implemented. Fulfillment, authentication/security, and a frontend are future work.

The first workflow connects Order and Inventory. Fulfillment will follow only after that workflow has been verified. Warexis centers on backend business coordination; a storefront is outside the initial scope.

## Ownership and communication

The domain boundaries assign orders, order lines, and the order lifecycle to Order, and stock items and reservations to Inventory. Each service controls its PostgreSQL database and Flyway migrations. Shared database tables, cross-service queries, and cross-service writes are prohibited.

REST handles client commands and queries. Order exposes create/read endpoints; Inventory exposes stock create/read/update endpoints. Their contracts are documented in the [Order guide](../../services/order-service/README.md) and [Inventory guide](../../services/inventory-service/README.md). Inventory decisions will travel asynchronously through Kafka; an Order request must not wait on a synchronous Inventory call to complete the business workflow.

The cross-service sequence is planned. Local order/outbox creation and Inventory reservation processing are implemented; no Kafka adapter connects them yet.

```mermaid
sequenceDiagram
    participant Client
    participant Order as Order Service
    participant Kafka
    participant Inventory as Inventory Service
    Client->>Order: Create order (REST)
    Note over Order: Commit PENDING_INVENTORY order and outbox event
    Order-->>Client: Order recorded; inventory pending
    Order->>Kafka: Publish OrderPlaced through outbox relay
    Kafka->>Inventory: Deliver OrderPlaced
    Note over Inventory: Commit reservation decision and outbox event
    Inventory->>Kafka: Publish InventoryReserved or InventoryRejected
    Kafka->>Order: Deliver inventory outcome
    Note over Order: Commit CONFIRMED or REJECTED transition
```

Publication and delivery are separate operations. Messages can be delayed or delivered more than once. A pending order is a valid intermediate state, not evidence that stock is available.

## Consistency and reliability

- **Eventual consistency:** Order and Inventory do not share a transaction. An inventory decision may be committed before Order reflects it.
- **At-least-once delivery:** Publication and consumer recovery can create duplicates. Consumers must tolerate repeated deliveries.
- **Transactional outbox:** Order creation and Inventory reservation processing commit domain changes and outgoing events in the same local PostgreSQL transaction. The future relay will publish committed events to Kafka, retaining event identity on retries.
- **Idempotent processing:** Inventory processed-event records commit with business changes and resulting outbox entries. A replay cannot reserve stock again. The future Order outcome handler must also persist deduplication with its lifecycle transition.
- **Business invariants:** Transport deduplication by `eventId` alone is insufficient if equivalent commands arrive with different event IDs. Inventory must also prevent multiple successful reservations for the same order.
- **Ordering:** Workflow events use `orderId` as the Kafka record key. Kafka ordering is scoped to a partition within a topic; this provides no global or cross-topic ordering guarantee. Producers and consumers must preserve the required per-order processing order and validate state transitions.

The [version 1 event contracts](event-contracts.md) and [ADR 0004](adr/0004-message-topology-and-delivery-semantics.md) select two capability streams, order keys, consumer groups, three-partition source/DLT topology, bounded technical retries, and explicit recovery. Consumers will acknowledge only after their local transaction commits, or after confirmed DLT publication for an unprocessable record. These transport and recovery mechanisms remain implementation work; the local foundations alone do not establish end-to-end delivery.

## Inventory transaction boundary

Reservation is all-or-nothing across an order's lines in one Inventory transaction. The internal application service validates original lines, aggregates demand for repeated SKUs, locks stock rows in deterministic order, and records the reservation decision, processed-event identity, and corresponding outcome outbox event. A rejected reservation leaves stock quantities unchanged. Unique order and event identities protect against repeated delivery; conflicting identities fail without business mutation. No public reservation HTTP endpoint or Kafka listener exists.

`available = onHand - reserved`. Reserving increases `reserved`; it does not reduce `onHand`. Confirmation in Order does not consume the stock a second time. Release and physical fulfillment behavior are future work.

## Future fulfillment

The planned extension is `OrderConfirmed → Kafka → Fulfillment Service`. Its data model, event contract, and lifecycle states are not yet defined or implemented. The initial Order lifecycle contains only `PENDING_INVENTORY`, `CONFIRMED`, and `REJECTED`.

## Related design

- [Domain model](domain-model.md)
- [Version 1 event contracts](event-contracts.md)
- [Service ownership decision](adr/0001-service-boundaries-and-database-ownership.md)
- [Messaging decision](adr/0002-asynchronous-kafka-communication.md)
- [Reliability decision](adr/0003-transactional-outbox-and-idempotent-consumers.md)
- [Topology and delivery decision](adr/0004-message-topology-and-delivery-semantics.md)
