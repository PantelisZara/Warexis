ALTER TABLE outbox_events ADD COLUMN traceparent VARCHAR(55);
ALTER TABLE fulfillments ADD COLUMN confirmation_traceparent VARCHAR(55);
