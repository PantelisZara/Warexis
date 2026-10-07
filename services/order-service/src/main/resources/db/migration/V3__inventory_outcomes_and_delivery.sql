CREATE TABLE inventory_decisions (
    order_id UUID PRIMARY KEY REFERENCES orders(id),
    reservation_id UUID NOT NULL UNIQUE,
    decision_json JSONB NOT NULL CHECK (jsonb_typeof(decision_json) = 'object')
);
CREATE TABLE processed_inventory_events (
    event_id UUID PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES inventory_decisions(order_id),
    event_json JSONB NOT NULL CHECK (jsonb_typeof(event_json) = 'object')
);
ALTER TABLE outbox_events ADD COLUMN publication_attempts INTEGER NOT NULL DEFAULT 0
    CHECK (publication_attempts BETWEEN 0 AND 5);
ALTER TABLE outbox_events ADD COLUMN next_attempt_at TIMESTAMPTZ;
CREATE TABLE messaging_relay_control (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    paused BOOLEAN NOT NULL DEFAULT FALSE,
    failure_classification VARCHAR(32)
);
INSERT INTO messaging_relay_control (id) VALUES (1);
CREATE TABLE messaging_deliveries (
    source_topic VARCHAR(249) NOT NULL,
    source_partition INTEGER NOT NULL CHECK (source_partition >= 0),
    source_offset BIGINT NOT NULL CHECK (source_offset >= 0),
    phase VARCHAR(16) NOT NULL DEFAULT 'PROCESS' CHECK (phase IN ('PROCESS','DLT','COMPLETE','PAUSED')),
    processing_attempts INTEGER NOT NULL DEFAULT 0 CHECK (processing_attempts BETWEEN 0 AND 4),
    dlt_attempts INTEGER NOT NULL DEFAULT 0 CHECK (dlt_attempts BETWEEN 0 AND 4),
    next_attempt_at TIMESTAMPTZ,
    failure_classification VARCHAR(32),
    PRIMARY KEY (source_topic, source_partition, source_offset)
);
