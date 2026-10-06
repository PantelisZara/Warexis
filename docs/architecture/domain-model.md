# Initial domain model

These are conceptual domain definitions, not implemented entities or a finalized database schema. Identifier storage types, constraints, API validation, and migrations will be specified during implementation.

## Order Service

| Model | Fields | Responsibility |
| --- | --- | --- |
| Order | `id`, `status`, `createdAt`, `updatedAt`, order lines | Own the lifecycle of one order |
| OrderLine | `id`, `sku`, `quantity` | Identify requested stock and quantity within an order |

An order needs at least one line, a non-empty SKU for each line, and positive integer quantities. Inventory must consider the total requested quantity for each SKU, including repeated SKUs across lines.

```mermaid
stateDiagram-v2
    [*] --> PENDING_INVENTORY: Order created
    PENDING_INVENTORY --> CONFIRMED: InventoryReserved
    PENDING_INVENTORY --> REJECTED: InventoryRejected
```

`CONFIRMED` means inventory was reserved. It does not mean payment, shipment, or fulfillment occurred. `REJECTED` means inventory could not be reserved. These are terminal outcomes for the initial workflow; cancellation, replacement orders, and future fulfillment states require later design.

Repeated outcome deliveries must not repeat a transition. An outcome that conflicts with the recorded decision must not overwrite it silently; handling that inconsistency is an implementation requirement.

## Inventory Service

| Model | Fields | Responsibility |
| --- | --- | --- |
| StockItem | `id`, `sku`, `onHand`, `reserved` | Track stock quantities for a SKU |
| InventoryReservation | `id`, `orderId`, `status`, lines | Record the reservation decision for an order |

Reservation lines conceptually retain the requested SKU, quantity, and source order-line identifier. Proposed reservation statuses are `RESERVED` and `REJECTED`; these are design terms, not existing enums. A rejected reservation retains the attempted lines for its decision record without reserving them.

### Invariants

- `onHand >= 0`, `reserved >= 0`, and `reserved <= onHand`.
- `available = onHand - reserved`.
- A successful reservation increases `reserved` for every requested SKU in a single transaction.
- Missing stock or insufficient availability rejects the entire reservation and leaves stock unchanged.
- Concurrent reservations must not oversell stock.
- The initial workflow has one reservation decision per `orderId`; repeated processing returns or preserves that decision without changing stock again.

Locking or conditional-update strategies and database constraints remain implementation choices. The all-or-nothing guarantee must be verified with real PostgreSQL concurrency tests.

Stock release, reservation expiry, stock adjustment interfaces, and fulfillment deductions are outside this initial model. See the [event contracts](event-contracts.md) for the messages connecting these domains.
