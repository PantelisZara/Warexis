package io.github.panteliszara.warexis.order.outbox;

import java.sql.Timestamp;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class OutboxRepository {

    private final JdbcClient jdbc;

    public OutboxRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void append(OutboxEvent event) {
        jdbc.sql("""
                INSERT INTO outbox_events
                    (event_id, aggregate_id, event_type, event_version, occurred_at, correlation_id, event_json)
                VALUES (:eventId, :aggregateId, :eventType, :eventVersion, :occurredAt, :correlationId,
                        CAST(:eventJson AS jsonb))
                """)
                .param("eventId", event.eventId())
                .param("aggregateId", event.aggregateId())
                .param("eventType", event.eventType())
                .param("eventVersion", event.eventVersion())
                .param("occurredAt", Timestamp.from(event.occurredAt()))
                .param("correlationId", event.correlationId())
                .param("eventJson", event.eventJson())
                .update();
    }
}
