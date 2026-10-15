package io.github.panteliszara.warexis.fulfillment.messaging;

import java.nio.charset.StandardCharsets;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Component
@ConditionalOnProperty(name = "warexis.messaging.enabled", havingValue = "true")
public class OutboxRelay {
    private static final Logger LOG = LoggerFactory.getLogger(OutboxRelay.class);
    private final io.github.panteliszara.warexis.fulfillment.observability.OperationalMetrics metrics;
    private final DataSource dataSource;
    private final JdbcClient jdbc;
    private final KafkaTemplate<byte[], byte[]> kafka;
    private final MessagingProperties properties;
    private final TransactionTemplate transaction;

    public OutboxRelay(DataSource dataSource, JdbcClient jdbc, KafkaTemplate<byte[], byte[]> kafka,
            MessagingProperties properties, PlatformTransactionManager transactions) {
        this(dataSource, jdbc, kafka, properties, transactions,
                new io.github.panteliszara.warexis.fulfillment.observability.OperationalMetrics(null, false));
    }

    @org.springframework.beans.factory.annotation.Autowired
    public OutboxRelay(DataSource dataSource, JdbcClient jdbc, KafkaTemplate<byte[], byte[]> kafka,
            MessagingProperties properties, PlatformTransactionManager transactions,
            io.github.panteliszara.warexis.fulfillment.observability.OperationalMetrics metrics) {
        this.metrics = metrics;
        this.dataSource = dataSource;
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.properties = properties;
        this.transaction = new TransactionTemplate(transactions);
    }

    @Scheduled(fixedDelayString = "${warexis.messaging.poll-interval:250ms}")
    public void publishNext() {
        // A dedicated session lock spans separate committed retry metadata and the broker call.
        try (var connection = dataSource.getConnection(); var lock = connection.createStatement()) {
            try (var row = lock.executeQuery("SELECT pg_try_advisory_lock(96, 1)")) {
                row.next();
                if (!row.getBoolean(1)) {
                    return;
                }
            }
            try {
                if (jdbc.sql("SELECT paused FROM messaging_relay_control WHERE id = 1").query(Boolean.class).single()) {
                    return;
                }
                var pending = jdbc.sql("""
                        SELECT event_id, correlation_id, event_json::text, publication_attempts, next_attempt_at
                        FROM outbox_events WHERE published_at IS NULL ORDER BY occurred_at, event_id LIMIT 1
                        """).query((rs, n) -> new Pending(rs.getObject(1, UUID.class), rs.getObject(2, UUID.class),
                        rs.getString(3), rs.getInt(4), rs.getTimestamp(5))).optional();
                if (pending.isEmpty()) {
                    return;
                }
                var event = pending.get();
                if (event.attempts() >= 5) {
                    pause("RETRY_EXHAUSTED", event, event.attempts());
                    return;
                }
                if (event.nextAt() != null && event.nextAt().toInstant().isAfter(Instant.now())) {
                    return;
                }
                var key = event.orderId().toString().getBytes(StandardCharsets.UTF_8);
                var body = event.json().getBytes(StandardCharsets.UTF_8);
                try {
                    EventJson.shipped(key, body);
                } catch (PoisonMessageException ex) {
                    pause("INVALID_EVENT", event, event.attempts());
                    return;
                }
                transaction.executeWithoutResult(status -> requireOne(jdbc.sql("""
                        UPDATE outbox_events SET publication_attempts = publication_attempts + 1, next_attempt_at = :next
                        WHERE event_id = :id AND published_at IS NULL AND publication_attempts < 5
                        """).param("id", event.id()).param("next", Timestamp.from(Instant.now().plus(
                        properties.retryBaseDelay().multipliedBy(1L << event.attempts())))).update()));
                if (event.attempts() > 0) metrics.publicationRetried();
                try {
                    kafka.send(KafkaConfiguration.FULFILLMENT_TOPIC, key, body).get(12, TimeUnit.SECONDS);
                    transaction.executeWithoutResult(status -> requireOne(jdbc.sql("""
                            UPDATE outbox_events SET published_at = GREATEST(CURRENT_TIMESTAMP, occurred_at), next_attempt_at = NULL
                            WHERE event_id = :id AND published_at IS NULL
                            """).param("id", event.id()).update()));
                    metrics.published();
                } catch (Exception ex) {
                    metrics.publicationFailed();
                    if (ex instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    if (event.attempts() + 1 >= 5) {
                        pause("RETRY_EXHAUSTED", event, event.attempts() + 1);
                    }
                    LOG.warn("Fulfillment relay publication remains pending; eventId={}, eventType=FulfillmentShipped, orderId={}, state=PENDING, attempt={}",
                            event.id(), event.orderId(), event.attempts() + 1);
                }
            } finally {
                try {
                    lock.execute("SELECT pg_advisory_unlock(96, 1)");
                } catch (SQLException ex) {
                    // Never return a possibly locked physical connection to the pool.
                    connection.abort(Runnable::run);
                    throw ex;
                }
            }
        } catch (Exception ex) {
            LOG.warn("Fulfillment relay cannot safely progress; retry metadata or session unavailable.");
        }
    }

    private void requireOne(int rows) {
        if (rows != 1) {
            throw new IllegalStateException("Outbox metadata update did not affect one pending event.");
        }
    }

    private void pause(String classification, Pending event, int attempt) {
        requireOne(jdbc.sql("UPDATE messaging_relay_control SET paused = TRUE, failure_classification = :classification WHERE id = 1")
                .param("classification", classification).update());
        LOG.error("Fulfillment relay paused; eventId={}, eventType=FulfillmentShipped, orderId={}, state=PAUSED, attempt={}, classification={}",
                event.id(), event.orderId(), attempt, classification);
    }

    private record Pending(UUID id, UUID orderId, String json, int attempts, Timestamp nextAt) {
    }
}
