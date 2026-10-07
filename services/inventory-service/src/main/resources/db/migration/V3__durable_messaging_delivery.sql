ALTER TABLE outbox_events ADD COLUMN publication_attempts INTEGER NOT NULL DEFAULT 0
    CHECK (publication_attempts BETWEEN 0 AND 5);
ALTER TABLE outbox_events ADD COLUMN next_attempt_at TIMESTAMPTZ;

CREATE TABLE outbox_relay_control (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    paused BOOLEAN NOT NULL DEFAULT FALSE,
    failure_code VARCHAR(32),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO outbox_relay_control (singleton) VALUES (TRUE);

CREATE TABLE consumer_delivery_state (
    consumer_group VARCHAR(128) NOT NULL,
    source_topic VARCHAR(249) NOT NULL,
    source_partition INTEGER NOT NULL CHECK (source_partition >= 0),
    source_offset BIGINT NOT NULL CHECK (source_offset >= 0),
    phase VARCHAR(16) NOT NULL DEFAULT 'PROCESS' CHECK (phase IN ('PROCESS', 'DLT', 'COMPLETE', 'PAUSED')),
    processing_attempts INTEGER NOT NULL DEFAULT 0 CHECK (processing_attempts BETWEEN 0 AND 4),
    dlt_attempts INTEGER NOT NULL DEFAULT 0 CHECK (dlt_attempts BETWEEN 0 AND 4),
    next_attempt_at TIMESTAMPTZ,
    failure_code VARCHAR(32),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, source_topic, source_partition, source_offset)
);
CREATE INDEX consumer_delivery_paused_idx ON consumer_delivery_state
    (consumer_group, source_topic, source_partition, source_offset) WHERE phase = 'PAUSED';
