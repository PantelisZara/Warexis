CREATE TABLE inventory_reservations (
    reservation_id UUID PRIMARY KEY,
    order_id UUID NOT NULL UNIQUE,
    source_event_id UUID NOT NULL UNIQUE,
    status VARCHAR(16) NOT NULL,
    reason_code VARCHAR(32),
    decided_at TIMESTAMPTZ NOT NULL,
    lines JSONB NOT NULL,
    CONSTRAINT inventory_reservations_decision_valid CHECK (
        (status = 'RESERVED' AND reason_code IS NULL) OR
        (status = 'REJECTED' AND reason_code IS NOT NULL AND
         reason_code IN ('UNKNOWN_SKU', 'INSUFFICIENT_STOCK'))
    ),
    CONSTRAINT inventory_reservations_lines_valid CHECK (
        jsonb_typeof(lines) = 'array' AND jsonb_array_length(lines) BETWEEN 1 AND 100
    )
);

CREATE TABLE processed_order_events (
    event_id UUID PRIMARY KEY,
    reservation_id UUID NOT NULL REFERENCES inventory_reservations(reservation_id),
    processed_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX processed_order_events_reservation_idx ON processed_order_events (reservation_id);

CREATE TABLE outbox_events (
    event_id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL UNIQUE REFERENCES inventory_reservations(reservation_id),
    event_type VARCHAR(64) NOT NULL CHECK (event_type IN ('InventoryReserved', 'InventoryRejected')),
    event_version INTEGER NOT NULL CHECK (event_version = 1),
    occurred_at TIMESTAMPTZ NOT NULL,
    correlation_id UUID NOT NULL,
    causation_id UUID NOT NULL,
    event_json JSONB NOT NULL CHECK (jsonb_typeof(event_json) = 'object'),
    published_at TIMESTAMPTZ,
    CONSTRAINT outbox_events_publication_valid CHECK (published_at IS NULL OR published_at >= occurred_at),
    CONSTRAINT outbox_events_envelope_matches CHECK ((
        event_json ->> 'eventId' = event_id::text AND
        event_json ->> 'aggregateId' = aggregate_id::text AND
        event_json ->> 'eventType' = event_type AND
        event_json -> 'eventVersion' = to_jsonb(event_version) AND
        event_json ->> 'correlationId' = correlation_id::text AND
        event_json ->> 'causationId' = causation_id::text AND
        (event_json ->> 'occurredAt')::timestamptz = occurred_at AND
        jsonb_typeof(event_json -> 'payload') = 'object' AND
        event_json -> 'payload' ->> 'orderId' = correlation_id::text AND
        event_json -> 'payload' ->> 'reservationId' = aggregate_id::text AND
        jsonb_typeof(event_json -> 'payload' -> 'lines') = 'array' AND
        jsonb_array_length(event_json -> 'payload' -> 'lines') BETWEEN 1 AND 100 AND
        ((event_type = 'InventoryReserved' AND NOT (event_json -> 'payload' ? 'reasonCode')) OR
         (event_type = 'InventoryRejected' AND event_json -> 'payload' ->> 'reasonCode'
          IN ('UNKNOWN_SKU', 'INSUFFICIENT_STOCK')))
    ) IS TRUE)
);
CREATE INDEX outbox_events_unpublished_idx ON outbox_events (occurred_at, event_id) WHERE published_at IS NULL;
