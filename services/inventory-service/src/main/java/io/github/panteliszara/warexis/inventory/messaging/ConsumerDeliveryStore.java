package io.github.panteliszara.warexis.inventory.messaging;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.OptionalLong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ConsumerDeliveryStore {
    public static final String GROUP = "warexis.inventory.order-events.v1";
    private final JdbcTemplate jdbc;
    private final MessagingProperties properties;

    public record State(String phase, int processingAttempts, int dltAttempts, Instant nextAttemptAt, String failureCode) {}

    public ConsumerDeliveryStore(JdbcTemplate jdbc, MessagingProperties properties) {
        this.jdbc = jdbc;
        this.properties = properties;
    }

    public State load(ConsumerRecord<byte[], byte[]> record) {
        jdbc.update("""
                INSERT INTO consumer_delivery_state (consumer_group, source_topic, source_partition, source_offset)
                VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING
                """, GROUP, record.topic(), record.partition(), record.offset());
        return jdbc.queryForObject("""
                SELECT * FROM consumer_delivery_state
                WHERE consumer_group = ? AND source_topic = ? AND source_partition = ? AND source_offset = ?
                """, (row, index) -> new State(row.getString("phase"), row.getInt("processing_attempts"),
                row.getInt("dlt_attempts"), row.getTimestamp("next_attempt_at") == null ? null : row.getTimestamp("next_attempt_at").toInstant(),
                row.getString("failure_code")), GROUP, record.topic(), record.partition(), record.offset());
    }

    public OptionalLong pausedOffset(TopicPartition partition) {
        var offset = jdbc.queryForObject("""
                SELECT min(source_offset) FROM consumer_delivery_state
                WHERE consumer_group = ? AND source_topic = ? AND source_partition = ? AND phase = 'PAUSED'
                """, Long.class, GROUP, partition.topic(), partition.partition());
        return offset == null ? OptionalLong.empty() : OptionalLong.of(offset);
    }

    /** Claim only the observed phase/budget; an overlapping owner must reload after a lost claim. */
    public boolean beginAttempt(ConsumerRecord<byte[], byte[]> record, State state, boolean dlt) {
        var phase = dlt ? "DLT" : "PROCESS";
        if (!state.phase().equals(phase)) return false;
        var attempts = dlt ? state.dltAttempts() : state.processingAttempts();
        var column = dlt ? "dlt_attempts" : "processing_attempts";
        var changed = jdbc.update("UPDATE consumer_delivery_state SET " + column + " = " + column + " + 1, next_attempt_at = ?, updated_at = now()"
                + " WHERE consumer_group = ? AND source_topic = ? AND source_partition = ? AND source_offset = ?"
                + " AND phase = ? AND " + column + " = ? AND " + column + " < 4",
                Timestamp.from(Instant.now().plus(properties.delay(attempts + 1))), GROUP,
                record.topic(), record.partition(), record.offset(), phase, attempts);
        if (changed == 0) {
            var current = progress(record);
            var currentAttempts = dlt ? current.dltAttempts() : current.processingAttempts();
            if (current.phase().equals(phase) && currentAttempts == attempts && attempts < 4) {
                throw new IllegalStateException("Delivery attempt was not persisted.");
            }
        }
        return changed == 1;
    }

    /** COMPLETE is terminal even when another owner is still handling an earlier delivery snapshot. */
    public boolean transition(ConsumerRecord<byte[], byte[]> record, String phase, String failureCode) {
        var predecessor = switch (phase) {
            case "DLT" -> "phase = 'PROCESS'";
            case "PAUSED" -> "phase = 'DLT' AND dlt_attempts >= 4";
            case "COMPLETE" -> "phase <> 'COMPLETE'";
            default -> throw new IllegalArgumentException("Unsupported delivery transition.");
        };
        var changed = jdbc.update("""
                UPDATE consumer_delivery_state SET phase = ?, failure_code = ?, next_attempt_at = NULL, updated_at = now()
                WHERE consumer_group = ? AND source_topic = ? AND source_partition = ? AND source_offset = ? AND
                """ + predecessor, phase, failureCode, GROUP, record.topic(), record.partition(), record.offset());
        if (changed == 0) {
            var current = progress(record);
            var stillEligible = switch (phase) {
                case "COMPLETE" -> !current.phase().equals("COMPLETE");
                case "DLT" -> current.phase().equals("PROCESS");
                case "PAUSED" -> current.phase().equals("DLT") && current.dltAttempts() >= 4;
                default -> false;
            };
            if (stillEligible) throw new IllegalStateException("Delivery transition was not persisted.");
        }
        return changed == 1;
    }

    private record Progress(String phase, int processingAttempts, int dltAttempts) {}

    private Progress progress(ConsumerRecord<byte[], byte[]> record) {
        var rows = jdbc.query("""
                SELECT phase, processing_attempts, dlt_attempts FROM consumer_delivery_state
                WHERE consumer_group = ? AND source_topic = ? AND source_partition = ? AND source_offset = ?
                """, (row, index) -> new Progress(row.getString("phase"), row.getInt("processing_attempts"), row.getInt("dlt_attempts")),
                GROUP, record.topic(), record.partition(), record.offset());
        if (rows.size() != 1) throw new IllegalStateException("Delivery recovery state unavailable.");
        return rows.getFirst();
    }
}
