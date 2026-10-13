# ADR 0005: Fulfillment ownership and manual dispatch

Status: Accepted and implemented. Verified with service tests and the local packaged three-service workflow.

## Context

Order owns inventory acceptance and currently ends at CONFIRMED or REJECTED. Inventory owns stock and immutable reservation decisions. A dispatch record needs a separate owner without shared databases or carrier integrations.

## Decision

Fulfillment owns one full-order record, its own PostgreSQL database and immutable original ordered line snapshot. Only OrderConfirmed triggers PENDING creation. A local manual ship command transitions PENDING to SHIPPED and commits one FulfillmentShipped transactional outbox fact. PENDING means awaiting manual dispatch; SHIPPED records a local operator's completed handoff using an internal shipment reference, without claiming carrier delivery.

The reference is globally unique within Fulfillment. Identical repeated commands return the original result. Changed terminal references or reuse conflict. No PROCESSING state is introduced because no distinct picking/packing operation exists in this bounded MVP. Partial shipping, cancellation, payment, customer/address data, carrier APIs and delivery confirmation are deferred.

Use producer-owned capability streams: OrderConfirmed joins warexis.order.events.v1; FulfillmentShipped belongs to warexis.fulfillment.events.v1. Inventory compatibility must precede confirmation publication. Fulfillment uses an independent Order-stream group and DLT. It validates unsubscribed OrderPlaced before durable acknowledgment and never treats InventoryReserved as a business trigger.

OrderConfirmed causation is the first accepted InventoryReserved event identity. Manual FulfillmentShipped has no causationId because its immediate trigger is a REST command without an event identity; payload.orderConfirmedEventId retains the workflow origin. Origin, reservation and ordered line identities remain immutable across aliases and dispatch.

Order remains CONFIRMED after dispatch. A future Order.SHIPPED feature would require Order's own independently validated FulfillmentShipped subscriber; Fulfillment never writes Order's database. Database locks, constraints, transaction boundaries and durable bounded retries provide local idempotent effects with at-least-once transport, not distributed exactly-once.

## Consequences

Each service can recover independently. The local API binds loopback. Its original unauthenticated boundary has since been replaced by the [stateless JWT authorization model](../security.md); manual dispatch now requires WAREXIS_OPERATOR. Production deployment/identity, stock consumption, carrier integration and delivery lifecycle decisions are required before expanding this boundary.
