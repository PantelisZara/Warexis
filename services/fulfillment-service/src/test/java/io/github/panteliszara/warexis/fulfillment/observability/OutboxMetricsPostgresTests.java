package io.github.panteliszara.warexis.fulfillment.observability;

import java.time.Clock;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class OutboxMetricsPostgresTests {
    @Container static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18.6");

    @Test void realPostgresSnapshotCapsBacklogUsesDatabaseAgeAndHandlesEmptyOrFutureEvents() throws Exception {
        var dataSource = new DriverManagerDataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE outbox_events(event_id BIGINT PRIMARY KEY, occurred_at TIMESTAMPTZ NOT NULL, published_at TIMESTAMPTZ)");
            statement.execute("CREATE INDEX ON outbox_events(occurred_at,event_id) WHERE published_at IS NULL");
            statement.execute("INSERT INTO outbox_events SELECT n, CURRENT_TIMESTAMP - INTERVAL '42 seconds', NULL FROM generate_series(1,10005) n");
        }
        var registry = new SimpleMeterRegistry();
        try (var snapshot = new OutboxMetricsSnapshot(dataSource, registry, Clock.systemUTC())) {
            snapshot.refresh();
            assertThat(snapshot.current().pending()).isEqualTo(10001);
            assertThat(snapshot.current().age()).isGreaterThanOrEqualTo(42);
            try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
                statement.execute("TRUNCATE outbox_events");
            }
            snapshot.refresh();
            assertThat(snapshot.current().pending()).isZero();
            assertThat(snapshot.current().age()).isZero();
            try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
                statement.execute("INSERT INTO outbox_events VALUES(1, CURRENT_TIMESTAMP + INTERVAL '1 day', NULL)");
            }
            snapshot.refresh();
            assertThat(snapshot.current().pending()).isEqualTo(1);
            assertThat(snapshot.current().age()).isZero();
        }
    }
}
