CREATE TABLE outbox_events (
    event_id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL REFERENCES orders(id),
    event_type VARCHAR(64) NOT NULL,
    event_version INTEGER NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    correlation_id UUID NOT NULL,
    causation_id UUID,
    event_json JSONB NOT NULL,
    published_at TIMESTAMPTZ,
    CONSTRAINT outbox_event_type_check CHECK (event_type ~ '^[A-Za-z][A-Za-z0-9]{0,63}$'),
    CONSTRAINT outbox_event_version_check CHECK (event_version > 0),
    CONSTRAINT outbox_aggregate_event_unique UNIQUE (aggregate_id, event_type, event_version),
    CONSTRAINT outbox_publication_time_check CHECK (published_at IS NULL OR published_at >= occurred_at),
    CONSTRAINT outbox_envelope_check CHECK ((
        jsonb_typeof(event_json) = 'object'
        AND event_json ->> 'eventId' = event_id::text
        AND event_json ->> 'aggregateId' = aggregate_id::text
        AND event_json ->> 'eventType' = event_type
        AND jsonb_typeof(event_json -> 'eventVersion') = 'number'
        AND (event_json ->> 'eventVersion')::integer = event_version
        AND (event_json ->> 'occurredAt')::timestamptz = occurred_at
        AND event_json ->> 'correlationId' = correlation_id::text
        AND ((causation_id IS NULL AND NOT event_json ? 'causationId')
             OR event_json ->> 'causationId' = causation_id::text)
        AND jsonb_typeof(event_json -> 'payload') = 'object'
        AND event_json -> 'payload' ->> 'orderId' = aggregate_id::text
        AND jsonb_typeof(event_json -> 'payload' -> 'lines') = 'array'
        AND jsonb_array_length(event_json -> 'payload' -> 'lines') BETWEEN 1 AND 100
    ) IS TRUE)
);

CREATE INDEX outbox_unpublished_poll_idx ON outbox_events (occurred_at, event_id)
    WHERE published_at IS NULL;
