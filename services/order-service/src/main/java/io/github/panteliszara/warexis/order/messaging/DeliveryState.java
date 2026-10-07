package io.github.panteliszara.warexis.order.messaging;

import java.sql.Timestamp;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@ConditionalOnProperty(name = "warexis.messaging.enabled", havingValue = "true")
public class DeliveryState {
    private final JdbcClient jdbc;
    private final MessagingProperties properties;

    public DeliveryState(JdbcClient jdbc, MessagingProperties properties) {
        this.jdbc = jdbc;
        this.properties = properties;
    }

    @Transactional
    public State load(ConsumerRecord<byte[], byte[]> record) {
        bind("""
                INSERT INTO messaging_deliveries (source_topic,source_partition,source_offset)
                VALUES (:topic,:partition,:offset) ON CONFLICT DO NOTHING
                """, record).update();
        return bind("""
                SELECT phase, processing_attempts, dlt_attempts, next_attempt_at, failure_classification
                FROM messaging_deliveries WHERE source_topic=:topic AND source_partition=:partition AND source_offset=:offset
                """, record).query((rs,n) -> new State(rs.getString(1), rs.getInt(2), rs.getInt(3),
                rs.getTimestamp(4), rs.getString(5))).single();
    }

    @Transactional
    public boolean begin(ConsumerRecord<byte[], byte[]> record, State state) {
        var column = state.phase().equals("PROCESS") ? "processing_attempts" : "dlt_attempts";
        int attempts = state.phase().equals("PROCESS") ? state.processingAttempts() : state.dltAttempts();
        int updated = bind("UPDATE messaging_deliveries SET " + column + "=" + column + "+1, next_attempt_at=:next"
                + " WHERE source_topic=:topic AND source_partition=:partition AND source_offset=:offset"
                + " AND phase=:phase AND " + column + "=:attempts AND " + column + "<4", record)
                .param("phase", state.phase()).param("attempts", attempts)
                .param("next", Timestamp.from(Instant.now().plus(properties.retryBaseDelay().multipliedBy(1L << attempts))))
                .update();
        if (updated == 1) {
            return true;
        }
        var current = current(record);
        int currentAttempts = column.equals("processing_attempts") ? current.processingAttempts() : current.dltAttempts();
        if (current.phase().equals(state.phase()) && currentAttempts == attempts) {
            throw new IllegalStateException("Durable delivery attempt was not recorded.");
        }
        return false;
    }

    @Transactional
    public void toDlt(ConsumerRecord<byte[], byte[]> record, String classification) {
        int updated = bind("""
                UPDATE messaging_deliveries SET phase='DLT', next_attempt_at=NULL, failure_classification=:failure
                WHERE source_topic=:topic AND source_partition=:partition AND source_offset=:offset AND phase='PROCESS'
                """, record).param("failure", classification).update();
        if (updated == 0 && current(record).phase().equals("PROCESS")) {
            throw new IllegalStateException("Durable DLT transition was not recorded.");
        }
    }

    @Transactional
    public void complete(ConsumerRecord<byte[], byte[]> record) {
        if (bind("""
                UPDATE messaging_deliveries SET phase='COMPLETE', next_attempt_at=NULL
                WHERE source_topic=:topic AND source_partition=:partition AND source_offset=:offset
                """, record).update() != 1) {
            throw new IllegalStateException("Missing durable delivery completion metadata.");
        }
    }

    @Transactional
    public boolean pause(ConsumerRecord<byte[], byte[]> record) {
        if (bind("""
                UPDATE messaging_deliveries SET phase='PAUSED'
                WHERE source_topic=:topic AND source_partition=:partition AND source_offset=:offset AND phase<>'COMPLETE'
                """, record).update() == 1) {
            return true;
        }
        if (current(record).phase().equals("COMPLETE")) {
            return false;
        }
        throw new IllegalStateException("Durable delivery pause was not recorded.");
    }

    @Transactional(readOnly = true)
    public java.util.Optional<Long> pausedOffset(org.apache.kafka.common.TopicPartition partition) {
        return jdbc.sql("""
                SELECT source_offset FROM messaging_deliveries WHERE source_topic=:topic
                AND source_partition=:partition AND phase='PAUSED' ORDER BY source_offset LIMIT 1
                """).param("topic", partition.topic()).param("partition", partition.partition())
                .query(Long.class).optional();
    }

    private State current(ConsumerRecord<byte[], byte[]> record) {
        return bind("""
                SELECT phase, processing_attempts, dlt_attempts, next_attempt_at, failure_classification
                FROM messaging_deliveries WHERE source_topic=:topic AND source_partition=:partition AND source_offset=:offset
                """, record).query((rs,n) -> new State(rs.getString(1), rs.getInt(2), rs.getInt(3),
                rs.getTimestamp(4), rs.getString(5))).optional()
                .orElseThrow(() -> new IllegalStateException("Missing durable delivery metadata."));
    }

    private JdbcClient.StatementSpec bind(String sql, ConsumerRecord<byte[], byte[]> record) {
        return jdbc.sql(sql).param("topic", record.topic()).param("partition", record.partition())
                .param("offset", record.offset());
    }

    public record State(String phase, int processingAttempts, int dltAttempts, Timestamp nextAt, String failure) {
    }
}
