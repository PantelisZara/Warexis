# ADR 0003: Transactional outbox and idempotent consumers

Status: Accepted. Local Order/Inventory outboxes and Inventory idempotent processing are implemented; transport adapters remain planned.

## Context

Updating PostgreSQL and publishing to Kafka are separate operations. A failure between them can leave committed business state without its event. A publisher retry or consumer restart can also cause duplicate delivery.

## Decision

Persist each outgoing event in a transactional outbox alongside the corresponding domain changes. Order creation and Inventory reservation processing now implement that local transaction boundary. A future relay will publish committed events and track publication progress. Retries retain the original `eventId` and event content.

Consumers persist processed event identifiers atomically with their business changes and any outgoing outbox events. Acknowledge processing only after commit. Enforce domain-level uniqueness as well, including one reservation decision per order in the initial workflow.

## Rationale

A local database transaction closes the gap between a business update and recording its publication intent. Durable deduplication and domain constraints make repeated deliveries safe without requiring exactly-once transport.

## Consequences and trade-offs

- A crash after Kafka publication but before the relay records success can publish a duplicate; consumers must remain idempotent.
- Outbox and deduplication storage need indexing, monitoring, and a retention policy compatible with supported replay windows.
- Concurrent duplicate processing must be controlled by database constraints and transactions.
- Relay ordering must preserve the required per-order publication order; assigning a Kafka key alone is insufficient.
- Phase 3 outboxes store complete JSON envelopes and minimal publication metadata, with uniqueness constraints and indexes for unpublished events. No relay, claiming/lease system, attempt counter, or cleanup runs yet.
- Relay implementation and durable bounded recovery remain the next milestone, as defined in [ADR 0004](0004-message-topology-and-delivery-semantics.md). Order outcome consumption/deduplication remains planned.
