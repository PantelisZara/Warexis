package io.github.panteliszara.warexis.inventory.messaging;

import io.github.panteliszara.warexis.inventory.observability.InventoryMetrics;
import io.github.panteliszara.warexis.inventory.observability.TraceParent;
import io.github.panteliszara.warexis.inventory.observability.TraceSupport;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Component;

@Component
@EnableScheduling
@ConditionalOnProperty(name = "warexis.messaging.enabled", havingValue = "true")
public class InventoryOutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(InventoryOutboxPublisher.class);
    private final DataSource dataSource;
    private final KafkaTemplate<byte[], byte[]> kafka;
    private final MessagingProperties properties;
    private final WorkflowEventValidator validator;
    private final InventoryMetrics metrics;
    private final TraceSupport tracing;

    public InventoryOutboxPublisher(DataSource dataSource, KafkaTemplate<byte[], byte[]> kafka,
                                    MessagingProperties properties, WorkflowEventValidator validator) {
        this(dataSource, kafka, properties, validator, InventoryMetrics.disabled());
    }

    public InventoryOutboxPublisher(DataSource dataSource, KafkaTemplate<byte[], byte[]> kafka,
                                    MessagingProperties properties, WorkflowEventValidator validator,
                                    InventoryMetrics metrics) {
        this(dataSource, kafka, properties, validator, metrics, TraceSupport.noop());
    }

    @Autowired
    public InventoryOutboxPublisher(DataSource dataSource, KafkaTemplate<byte[], byte[]> kafka,
                                    MessagingProperties properties, WorkflowEventValidator validator,
                                    InventoryMetrics metrics, TraceSupport tracing) {
        this.tracing = tracing;
        this.metrics = metrics;
        this.dataSource = dataSource;
        this.kafka = kafka;
        this.properties = properties;
        this.validator = validator;
    }

    @Scheduled(fixedDelayString = "${warexis.messaging.poll-interval}")
    public void publishNext() {
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(true);
            if (!lock(connection)) return;
            try { publishHead(connection); }
            finally {
                try (var statement = connection.prepareStatement("SELECT pg_advisory_unlock(90, 1)")) { statement.execute(); }
                catch (SQLException exception) { connection.abort(Runnable::run); throw exception; }
            }
        } catch (Exception exception) {
            log.error("Inventory relay halted this poll: durable metadata unavailable.");
        }
    }

    private boolean lock(Connection connection) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT pg_try_advisory_lock(90, 1)")) {
            try (var result = statement.executeQuery()) { result.next(); return result.getBoolean(1); }
        }
    }

    private void publishHead(Connection connection) throws Exception {
        try (var statement = connection.prepareStatement("SELECT paused FROM outbox_relay_control WHERE singleton = TRUE")) {
            try (var result = statement.executeQuery()) {
                if (!result.next() || result.getBoolean(1)) return;
            }
        }
        try (var statement = connection.prepareStatement("SELECT * FROM outbox_events WHERE published_at IS NULL ORDER BY occurred_at, event_id LIMIT 1")) {
            try (var row = statement.executeQuery()) {
                if (!row.next()) return;
                var id = row.getObject("event_id", java.util.UUID.class);
                var attempts = row.getInt("publication_attempts");
                if (attempts >= 5) { pause(connection, "RETRY_EXHAUSTED"); return; }
                var due = row.getTimestamp("next_attempt_at");
                if (due != null && due.toInstant().isAfter(Instant.now())) return;
                var key = row.getObject("correlation_id", java.util.UUID.class).toString().getBytes(StandardCharsets.UTF_8);
                var value = row.getString("event_json").getBytes(StandardCharsets.UTF_8);
                try {
                    var event = validator.validate(key, value);
                    if (!event.eventType().equals("InventoryReserved") && !event.eventType().equals("InventoryRejected")) throw new InvalidEventException();
                } catch (InvalidEventException exception) { pause(connection, "INVALID_CONTRACT"); return; }
                try (var update = connection.prepareStatement("UPDATE outbox_events SET publication_attempts = publication_attempts + 1, next_attempt_at = ? WHERE event_id = ? AND publication_attempts < 5")) {
                    update.setTimestamp(1, Timestamp.from(Instant.now().plus(properties.delay(attempts + 1))));
                    update.setObject(2, id);
                    if (update.executeUpdate() != 1) throw new SQLException("Publication retry budget unavailable.");
                }
                log.info("Inventory relay eventId={} eventType={} orderId={} aggregateId={} attempt={} state=SEND",
                        id, row.getString("event_type"), row.getObject("correlation_id", java.util.UUID.class),
                        row.getObject("aggregate_id", java.util.UUID.class), attempts + 1);
                if (attempts > 0) metrics.outboxRetry();
                var parent = TraceParent.normalize(row.getString("traceparent"));
                var eventType = row.getString("event_type");
                try {
                    tracing.attempt(TraceSupport.Operation.PUBLISH, parent, eventType, () -> {
                        var record = new ProducerRecord<byte[], byte[]>("warexis.inventory.events.v1", key, value);
                        TraceParent.inject(record, tracing.currentTraceparent());
                        kafka.send(record).get(11, TimeUnit.SECONDS);
                        // This connection is autocommit: span success follows the durable metadata write.
                        try (var update = connection.prepareStatement("UPDATE outbox_events SET published_at = GREATEST(CURRENT_TIMESTAMP, occurred_at), next_attempt_at = NULL WHERE event_id = ?")) {
                            update.setObject(1, id);
                            if (update.executeUpdate() != 1) throw new SQLException("Publication metadata unavailable.");
                        }
                        return null;
                    });
                } catch (SQLException exception) {
                    metrics.publication(false);
                    throw exception;
                } catch (Exception exception) {
                    metrics.publication(false);
                    log.warn("Inventory relay eventId={} eventType={} orderId={} attempt={} state=RETRY classification=BROKER_UNAVAILABLE",
                            id, eventType, row.getObject("correlation_id", java.util.UUID.class), attempts + 1);
                    if (exception instanceof InterruptedException) Thread.currentThread().interrupt();
                    if (attempts + 1 >= 5) pause(connection, "RETRY_EXHAUSTED");
                    return;
                }
                metrics.publication(true);
                log.info("Inventory relay eventId={} eventType={} orderId={} attempt={} state=PUBLISHED",
                        id, row.getString("event_type"), row.getObject("correlation_id", java.util.UUID.class), attempts + 1);
            }
        }
    }

    private void pause(Connection connection, String code) throws SQLException {
        try (var statement = connection.prepareStatement("UPDATE outbox_relay_control SET paused = TRUE, failure_code = ?, updated_at = now() WHERE singleton = TRUE")) {
            statement.setString(1, code);
            if (statement.executeUpdate() != 1) throw new SQLException("Relay recovery state unavailable.");
        }
        log.error("Inventory relay durably paused: {}.", code);
    }
}
