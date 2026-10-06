# ADR 0003: Transactional outbox and idempotent consumers

Status: Accepted architectural direction; implementation pending.

## Context

Updating PostgreSQL and publishing to Kafka are separate operations. A failure between them can leave committed business state without its event. A publisher retry or consumer restart can also cause duplicate delivery.

## Decision

When messaging is implemented, persist each outgoing event in a transactional outbox alongside the corresponding domain changes. A relay publishes committed events and tracks publication progress. Retries retain the original `eventId` and event content.

Consumers persist processed event identifiers atomically with their business changes and any outgoing outbox events. Acknowledge processing only after commit. Enforce domain-level uniqueness as well, including one reservation decision per order in the initial workflow.

## Rationale

A local database transaction closes the gap between a business update and recording its publication intent. Durable deduplication and domain constraints make repeated deliveries safe without requiring exactly-once transport.

## Consequences and trade-offs

- A crash after Kafka publication but before the relay records success can publish a duplicate; consumers must remain idempotent.
- Outbox and deduplication storage need indexing, monitoring, and a retention policy compatible with supported replay windows.
- Concurrent duplicate processing must be controlled by database constraints and transactions.
- Relay ordering must preserve the required per-order publication order; assigning a Kafka key alone is insufficient.
- Relay implementation, cleanup, and recovery tooling add complexity. None is implemented by this documentation task.
