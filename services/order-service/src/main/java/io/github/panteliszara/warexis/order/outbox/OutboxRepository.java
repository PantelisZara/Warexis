package io.github.panteliszara.warexis.order.outbox;

import java.sql.Timestamp;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class OutboxRepository {

    private final JdbcClient jdbc;

    private final io.github.panteliszara.warexis.order.observability.TraceSupport tracing;

    public OutboxRepository(JdbcClient jdbc) {
        this(jdbc, io.github.panteliszara.warexis.order.observability.TraceSupport.noop());
    }

    @org.springframework.beans.factory.annotation.Autowired
    public OutboxRepository(JdbcClient jdbc, io.github.panteliszara.warexis.order.observability.TraceSupport tracing) {
        this.jdbc = jdbc;
        this.tracing = tracing;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void append(OutboxEvent event) {
        String traceparent = io.github.panteliszara.warexis.order.observability.TraceParent.normalize(tracing.currentTraceparent());
        var inserted = jdbc.sql("""
                INSERT INTO outbox_events
                    (event_id, aggregate_id, event_type, event_version, occurred_at, correlation_id, causation_id, event_json, traceparent)
                VALUES (:eventId, :aggregateId, :eventType, :eventVersion, :occurredAt, :correlationId, :causationId,
                        CAST(:eventJson AS jsonb), :traceparent)
                """)
                .param("eventId", event.eventId())
                .param("aggregateId", event.aggregateId())
                .param("eventType", event.eventType())
                .param("eventVersion", event.eventVersion())
                .param("occurredAt", Timestamp.from(event.occurredAt()))
                .param("correlationId", event.correlationId())
                .param("causationId", event.causationId())
                .param("eventJson", event.eventJson())
                .param("traceparent", traceparent, java.sql.Types.VARCHAR)
                .update();
        if (inserted != 1) {
            throw new IllegalStateException("Outbox append did not persist one event.");
        }
    }
}
