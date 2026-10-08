# Running and recovering the three-service Kafka workflow

Kafka adapters are opt-in. All three services default to `WAREXIS_MESSAGING_ENABLED=false`.
Set `WAREXIS_MESSAGING_ENABLED=true` and `KAFKA_BOOTSTRAP_SERVERS` for each service
process to enable its publisher and listener. Follow the
[local infrastructure guide](../../infrastructure/local/README.md) to start the
local PostgreSQL databases, Kafka broker, and host services. The unauthenticated
APIs bind to loopback in those recipes.

Enabled services provision the three-partition, single-replica development topics
from [ADR 0004](adr/0004-message-topology-and-delivery-semantics.md) and the Fulfillment extension in [ADR 0005](adr/0005-fulfillment-and-manual-dispatch.md). These settings
are local development defaults, not a production durability plan. Kafka uses
plaintext local transport. Database ownership stays separate; no service reads
another service's database.

`POST /api/orders` atomically writes an order and its `OrderPlaced` outbox record,
then returns `PENDING_INVENTORY`. The Order relay publishes the stored envelope.
Inventory validates the byte-array transport as strict UTF-8 JSON and calls its
transactional reservation application service. Its outcome outbox relay publishes
`InventoryReserved` or `InventoryRejected`. Order validates outcome identities,
causation, original lines, and permitted state transitions before atomically
recording the decision, deduplication and terminal state. Success also records one
`OrderConfirmed` outbox fact; rejection does not. Poll `GET /api/orders/{id}` for
the final inventory decision.

Fulfillment's independent Order-stream group creates one `PENDING` plan from
`OrderConfirmed`, preserving the original reservation and ordered lines. Poll
`GET /api/fulfillments/orders/{orderId}`; it can return 404 before asynchronous
planning completes. `POST /api/fulfillments/{id}/ship` with
`{"shipmentReference":"MANUAL-001"}` commits `SHIPPED` and one
`FulfillmentShipped` outbox fact. The reference is internal, case-sensitive, and
unique within Fulfillment. Identical repeats preserve the first result; changed
terminal references or reference reuse return 409. Order stays `CONFIRMED`;
dispatch does not deduct stock or contact a carrier. No current service consumes
`FulfillmentShipped`.

Inventory validates and acknowledges recognized `OrderConfirmed` without business
processing. Fulfillment validates and acknowledges `OrderPlaced` without planning.
Their independent groups have separate offsets and DLTs. Install Inventory's
compatibility before enabling Order's new confirmation producer.

`eventId` identifies a message; `orderId` enforces one business reservation
decision. Database constraints and transaction locks protect those identities.
Identical redelivery and matching event aliases reuse the existing decision.
Contradictory Inventory outcomes in either direction preserve the original
terminal Order state and go directly to DLT. A valid outcome for an unknown order
also goes directly to DLT. These are contract failures, rather than technical
retry or business rejection. Inventory locks overlapping SKUs in deterministic
case-sensitive order; stock updates, reservation, processed event and outgoing
outbox record commit or roll back together.

Producers retain event IDs and stored values on retries. Listeners acknowledge
source offsets after their application transaction and durable delivery completion
commit, or after confirmed DLT publication and durable delivery completion.
Byte-array keys/values let DLTs retain malformed UTF-8 and unparseable payloads. DLT headers use `warexis-source-topic`,
`warexis-source-partition`, `warexis-source-offset`, `warexis-consumer-group`,
`warexis-failure-code`, `warexis-processing-attempts`, `warexis-dlt-attempts`, and
`warexis-failed-at`. Identify a DLT record by the complete source topic, partition,
offset, and consumer group; identical payloads and keys can belong to earlier
source deliveries. No DLT redrive consumer or cleanup task runs automatically.

## Bounded failure handling

Relay attempts are persisted before the broker call. Five unsuccessful attempts
pause the service relay, retaining the unpublished row. The relay uses a database
session advisory lock to serialize publication across service replicas; it does
not skip a failed row. Consumers persist per-source-record processing and DLT
attempt budgets. Four unsuccessful processing attempts move the record to DLT;
four unsuccessful DLT attempts pause that partition and leave its offset
uncommitted. Contract/identity conflicts go directly to DLT. Business rejection
is successful processing and does not use technical retries.

The base retry delay is one second and publication polls every 250 milliseconds.
For controlled tests these are configurable with `warexis.messaging.retry-base-delay`
and `warexis.messaging.poll-interval`. Changing delays does not reset durable
budgets. A process restart preserves exhausted budgets and paused records.

Actuator health confirms that the process/database are available; it does not
certify workflow progress. Inspect service logs, pending outbox rows, relay control
rows, and delivery state to diagnose a pending order or paused stream.

| Service database | Relay control | Consumer delivery state |
| --- | --- | --- |
| Order | `messaging_relay_control` (`id = 1`) | `messaging_deliveries` |
| Inventory | `outbox_relay_control` (`singleton = true`) | `consumer_delivery_state` |
| Fulfillment | `messaging_relay_control` (`id = 1`) | `messaging_deliveries` |

All three outboxes expose `publication_attempts`, `next_attempt_at`, and `published_at`.
Delivery rows retain source topic/partition/offset, phase, processing/DLT attempt
counts, and due time. Inventory and Fulfillment include the consumer group in their keys; Order has
one fixed outcome group per owned database. No retention or deletion policy is
implemented, so do not manually remove deduplication/decision records while
replay remains possible.

## Explicit recovery

Recovery is a deliberate operator action after diagnosis. Stop the affected
service, repair the underlying broker/database/contract problem, then connect to
that service's own database using its configured local credentials. Never reset
budgets repeatedly without fixing the cause. Restart after the SQL transaction
commits. Retrying publication preserves the original event identity.

For a paused relay, reset the diagnosed unpublished event using its UUID. In Order:

```sql
BEGIN;
UPDATE outbox_events SET publication_attempts = 0, next_attempt_at = NULL
WHERE event_id = 'REPLACE_WITH_DIAGNOSED_EVENT_UUID'::uuid AND published_at IS NULL;
UPDATE messaging_relay_control SET paused = false, failure_classification = NULL
WHERE id = 1;
COMMIT;
```

Fulfillment uses the same relay recovery SQL as Order, in its own database.
In Inventory, use the same outbox update and replace the control update with:

```sql
UPDATE outbox_relay_control SET paused = false, failure_code = NULL, updated_at = now()
WHERE singleton = true;
```

For a partition paused because its DLT was unavailable, repair the DLT destination
and explicitly resume the diagnosed source record in phase `DLT`. In Order:

```sql
UPDATE messaging_deliveries
SET phase = 'DLT', dlt_attempts = 0, next_attempt_at = NULL
WHERE source_topic = 'warexis.inventory.events.v1'
  AND source_partition = REPLACE_PARTITION AND source_offset = REPLACE_OFFSET
  AND phase = 'PAUSED';
```

In Inventory:

```sql
UPDATE consumer_delivery_state
SET phase = 'DLT', dlt_attempts = 0, next_attempt_at = NULL, updated_at = now()
WHERE consumer_group = 'warexis.inventory.order-events.v1'
  AND source_topic = 'warexis.order.events.v1'
  AND source_partition = REPLACE_PARTITION AND source_offset = REPLACE_OFFSET
  AND phase = 'PAUSED';
```

In Fulfillment, use its owned database and independent group:

```sql
UPDATE messaging_deliveries
SET phase = 'DLT', dlt_attempts = 0, next_attempt_at = NULL
WHERE consumer_group = 'warexis.fulfillment.order-events.v1'
  AND source_topic = 'warexis.order.events.v1'
  AND source_partition = REPLACE_PARTITION AND source_offset = REPLACE_OFFSET
  AND phase = 'PAUSED';
```

If delivery metadata was unavailable, repair the database and inspect the persisted
phase/counters before restarting; do not assume the application transaction failed.
A broker-acknowledged publication or committed application operation can replay
safely through the normal idempotent handler. Redriving a diagnosed DLT record must
preserve the original key and bytes and use the normal source handler. No redrive
CLI or HTTP recovery endpoint is included. Late redrive can follow newer records;
terminal state and identity validation prevent a contradictory decision.

## Verification

Run each service's `./mvnw test` and `./mvnw verify`, then run the
[packaged-service Kafka workflow suite](../../tests/workflow/README.md).
These suites use disposable real PostgreSQL/Kafka infrastructure. Service tests
repeatedly synchronize oversubscribed reservations, overlapping/reversed SKU
inputs and concurrent duplicate event IDs or same-order aliases, and reconcile
stock against committed reservation lines. Test-only database faults verify
rollback at stock, reservation, processed-event and outbox persistence boundaries.

The packaged suite runs actual executable JARs and checks all three consumer directions
for pending-outbox recovery, forced termination after business commit but before
successful delivery checkpoint, temporary database outage with uncommitted source
offsets, and exhausted DLT budgets. It proves a paused partition remains blocked
across polls and JVM restart while another partition progresses. Explicit recovery
then completes several later order IDs on the same partition. Restarting again
with a new same-partition record proves completed DLT state does not resurrect.
Broker-outage scenarios also retain unpublished event identity and an exhausted
relay budget across Order or Fulfillment restart until explicit operator recovery.
The 47-case packaged suite preserves the original 28 Order/Inventory cases and
adds confirmation, manual dispatch, duplicate/alias/conflict handling, immutable
shipment origin, reference ownership, pending confirmation/shipment outbox
recovery, technical rollback, and Fulfillment restart/DLT/paused-partition checks.
These run against the current three executable JARs, separately from full service
unit and integration lifecycles.

These checks establish at-least-once delivery with idempotent local effects for
the tested boundaries. Broker acceptance before a database publication checkpoint,
or a business commit before source acknowledgment, can cause safe replay. Ordering
is scoped to a Kafka partition with `orderId` as key; unrelated partitions have no
global order. Paused records retain their source offset and durable budget. A
successful business rejection uses one normal processing attempt and no technical
retry. Database recovery checks restore the database and explicitly restart the
service; they do not promise automatic recovery from every connection failure.

This is local single-broker verification, not a production durability or capacity
claim. Replicated-broker failover, permanent database/storage loss and deployment
scale limits are outside these tests. No distributed exactly-once guarantee or
unlimited scalability is claimed. Hosted GitHub Actions execution remains pending
until repository publication.
