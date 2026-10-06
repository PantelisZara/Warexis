# ADR 0001: Service boundaries and database ownership

Status: Accepted. Independent services, core domains, and database ownership are established; local reservation processing is implemented, while cross-service Kafka delivery remains planned.

## Context

Order lifecycle management and stock reservation have different business invariants. Combining their data ownership would couple changes and allow services to bypass each other's rules. The initial workflow needs only these two domains.

## Decision

Order Service owns orders and order lines. Inventory Service owns stock and inventory reservations. Each owns its PostgreSQL database, schema changes, and local transactions. Services never directly read or modify another service's database.

Services exchange REST responses for client interactions and events for workflow coordination. Fulfillment Service will be introduced only after the Order/Inventory asynchronous workflow is proven.

## Rationale

Explicit ownership keeps lifecycle and stock invariants within the service responsible for enforcing them. Delaying Fulfillment keeps the initial implementation focused on one complete distributed workflow.

## Consequences and trade-offs

- Cross-service changes require asynchronous coordination and eventual consistency.
- Clients cannot rely on cross-database joins or a shared transaction.
- Each service needs its own migrations, integration tests, and recovery behavior.
- Separate database ownership does not require separate physical PostgreSQL hosts; any shared host must still enforce isolated databases and access.
