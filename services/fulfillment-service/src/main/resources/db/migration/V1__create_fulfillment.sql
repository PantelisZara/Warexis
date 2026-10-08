CREATE TABLE fulfillments (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL UNIQUE,
    reservation_id UUID NOT NULL UNIQUE,
    confirmation_event_id UUID NOT NULL UNIQUE,
    confirmation_causation_id UUID NOT NULL,
    status VARCHAR(16) NOT NULL CHECK (status IN ('PENDING','SHIPPED')),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL CHECK (updated_at >= created_at),
    shipped_at TIMESTAMPTZ,
    shipment_reference VARCHAR(64) UNIQUE CHECK (shipment_reference ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$'),
    CHECK ((status='PENDING' AND shipped_at IS NULL AND shipment_reference IS NULL)
        OR (status='SHIPPED' AND shipped_at IS NOT NULL AND shipment_reference IS NOT NULL AND shipped_at >= created_at))
);
CREATE TABLE fulfillment_lines (
    fulfillment_id UUID NOT NULL REFERENCES fulfillments(id),
    order_line_id UUID NOT NULL,
    line_position INTEGER NOT NULL CHECK (line_position BETWEEN 0 AND 99),
    sku VARCHAR(64) NOT NULL CHECK (sku ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$'),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (fulfillment_id,order_line_id),
    UNIQUE (fulfillment_id,line_position)
);
CREATE TABLE processed_confirmations (
    event_id UUID PRIMARY KEY,
    fulfillment_id UUID NOT NULL REFERENCES fulfillments(id),
    event_json JSONB NOT NULL CHECK (jsonb_typeof(event_json)='object'),
    processed_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE outbox_events (
    event_id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL REFERENCES fulfillments(id),
    event_type VARCHAR(64) NOT NULL CHECK (event_type='FulfillmentShipped'),
    event_version INTEGER NOT NULL CHECK (event_version=1),
    occurred_at TIMESTAMPTZ NOT NULL,
    correlation_id UUID NOT NULL,
    causation_id UUID CHECK (causation_id IS NULL),
    event_json JSONB NOT NULL,
    published_at TIMESTAMPTZ CHECK (published_at IS NULL OR published_at >= occurred_at),
    publication_attempts INTEGER NOT NULL DEFAULT 0 CHECK (publication_attempts BETWEEN 0 AND 5),
    next_attempt_at TIMESTAMPTZ,
    UNIQUE (aggregate_id,event_type,event_version),
    CHECK ((jsonb_typeof(event_json)='object'
        AND event_json->>'eventId'=event_id::text
        AND event_json->>'aggregateId'=aggregate_id::text
        AND event_json->>'eventType'=event_type
        AND event_json->'eventVersion'='1'::jsonb
        AND (event_json->>'occurredAt')::timestamptz=occurred_at
        AND event_json->>'correlationId'=correlation_id::text
        AND NOT event_json ? 'causationId'
        AND jsonb_typeof(event_json->'payload')='object'
        AND event_json->'payload'->>'fulfillmentId'=aggregate_id::text
        AND event_json->'payload'->>'orderId'=correlation_id::text
        AND (event_json->'payload'->>'shippedAt')::timestamptz=occurred_at
        AND jsonb_typeof(event_json->'payload'->'lines')='array'
        AND jsonb_array_length(event_json->'payload'->'lines') BETWEEN 1 AND 100) IS TRUE)
);
CREATE INDEX outbox_unpublished_poll_idx ON outbox_events(occurred_at,event_id) WHERE published_at IS NULL;
CREATE TABLE messaging_relay_control (
    id INTEGER PRIMARY KEY CHECK (id=1),
    paused BOOLEAN NOT NULL DEFAULT FALSE,
    failure_classification VARCHAR(32)
);
INSERT INTO messaging_relay_control(id) VALUES(1);
CREATE TABLE messaging_deliveries (
    consumer_group VARCHAR(249) NOT NULL DEFAULT 'warexis.fulfillment.order-events.v1',
    source_topic VARCHAR(249) NOT NULL,
    source_partition INTEGER NOT NULL CHECK (source_partition >= 0),
    source_offset BIGINT NOT NULL CHECK (source_offset >= 0),
    phase VARCHAR(16) NOT NULL DEFAULT 'PROCESS' CHECK (phase IN ('PROCESS','DLT','COMPLETE','PAUSED')),
    processing_attempts INTEGER NOT NULL DEFAULT 0 CHECK (processing_attempts BETWEEN 0 AND 4),
    dlt_attempts INTEGER NOT NULL DEFAULT 0 CHECK (dlt_attempts BETWEEN 0 AND 4),
    next_attempt_at TIMESTAMPTZ,
    failure_classification VARCHAR(32),
    PRIMARY KEY(consumer_group,source_topic,source_partition,source_offset)
);
CREATE INDEX messaging_paused_idx ON messaging_deliveries(consumer_group,source_topic,source_partition,source_offset) WHERE phase='PAUSED';
