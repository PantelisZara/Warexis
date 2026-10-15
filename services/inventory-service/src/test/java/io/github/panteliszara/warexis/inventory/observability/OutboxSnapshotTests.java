package io.github.panteliszara.warexis.inventory.observability;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import javax.sql.DataSource;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.*;

class OutboxSnapshotTests {
    @Test
    void boundedSnapshotIsSharedAndUnavailableNeverPretendsToBeZero() throws Exception {
        var source = mock(DataSource.class);
        var connection = mock(Connection.class);
        var statement = mock(PreparedStatement.class);
        var rows = mock(ResultSet.class);
        when(source.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(contains("LIMIT 10001"))).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(rows);
        when(rows.next()).thenReturn(true);
        var now = Instant.parse("2026-01-01T00:00:00Z");
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);
        var registry = new SimpleMeterRegistry();
        try {
            var snapshot = new OutboxSnapshot(source, registry, clock, false);
            try {
                assertThat(registry.get("warexis.outbox.pending").gauge().value()).isNaN();
                assertThat(registry.get("warexis.outbox.snapshot.available").gauge().value()).isZero();
                snapshot.refresh();
                assertThat(registry.get("warexis.outbox.pending").gauge().value()).isZero();
                assertThat(registry.get("warexis.outbox.oldest.age").gauge().value()).isZero();
                assertThat(registry.get("warexis.outbox.snapshot.available").gauge().value()).isEqualTo(1);
                when(rows.getDouble(1)).thenReturn(10001d);
                when(rows.getDouble(2)).thenReturn(42d);
                snapshot.refresh();
                assertThat(registry.get("warexis.outbox.pending").gauge().value()).isEqualTo(10001);
                assertThat(registry.get("warexis.outbox.oldest.age").gauge().value()).isEqualTo(42);
                verify(statement, times(2)).setQueryTimeout(2);
                verify(source, times(2)).getConnection(); // Gauges themselves never access the database.
                when(clock.instant()).thenReturn(now.plusSeconds(30));
                assertThat(registry.get("warexis.outbox.pending").gauge().value()).isNaN();
                when(source.getConnection()).thenThrow(new SQLException("unavailable"));
                snapshot.refresh();
                assertThat(registry.get("warexis.outbox.snapshot.available").gauge().value()).isZero();
                assertThat(registry.get("warexis.outbox.oldest.age").gauge().value()).isNaN();
            } finally { snapshot.stop(); }
        } finally { registry.close(); }
    }
}
