package io.github.panteliszara.warexis.inventory.observability;

import java.time.Clock;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class OutboxSnapshotPostgresTests {
    @Container static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18.6");

    @Test
    void realPostgresSnapshotCapsCountExcludesPublishedAndClampsFutureAge() {
        var source = new DriverManagerDataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        var jdbc = new JdbcTemplate(source);
        jdbc.execute("CREATE TABLE outbox_events (event_id bigint PRIMARY KEY, occurred_at timestamptz NOT NULL, published_at timestamptz)");
        jdbc.execute("CREATE INDEX outbox_events_unpublished_idx ON outbox_events (occurred_at, event_id) WHERE published_at IS NULL");
        var registry = new SimpleMeterRegistry();
        var snapshot = new OutboxSnapshot(source, registry, Clock.systemUTC(), false);
        try {
            snapshot.refresh();
            assertThat(registry.get("warexis.outbox.pending").gauge().value()).isZero();
            assertThat(registry.get("warexis.outbox.oldest.age").gauge().value()).isZero();
            jdbc.update("INSERT INTO outbox_events SELECT id, CURRENT_TIMESTAMP - INTERVAL '1 minute', NULL FROM generate_series(1, 10004) id");
            jdbc.update("INSERT INTO outbox_events VALUES (20000, CURRENT_TIMESTAMP - INTERVAL '1 year', CURRENT_TIMESTAMP)");
            snapshot.refresh();
            assertThat(registry.get("warexis.outbox.pending").gauge().value()).isEqualTo(10001);
            assertThat(registry.get("warexis.outbox.oldest.age").gauge().value()).isBetween(60d, 600d);
            jdbc.update("UPDATE outbox_events SET occurred_at = CURRENT_TIMESTAMP + INTERVAL '1 day' WHERE published_at IS NULL");
            snapshot.refresh();
            assertThat(registry.get("warexis.outbox.oldest.age").gauge().value()).isZero();
            jdbc.update("UPDATE outbox_events SET published_at = CURRENT_TIMESTAMP");
            snapshot.refresh();
            assertThat(registry.get("warexis.outbox.pending").gauge().value()).isZero();
        } finally {
            snapshot.stop();
            registry.close();
        }
    }
}
