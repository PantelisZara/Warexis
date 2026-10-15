package io.github.panteliszara.warexis.fulfillment.observability;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import javax.sql.DataSource;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class OutboxMetricsSnapshotTests {
    @Test void boundedSnapshotSharesOneQueryAndExportsAgeAndCount() throws Exception {
        var dataSource = mock(DataSource.class);
        var connection = mock(Connection.class);
        var statement = mock(Statement.class);
        var row = mock(ResultSet.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(row);
        when(row.next()).thenReturn(true);
        when(row.getDouble(1)).thenReturn(10001d);
        when(row.getDouble(2)).thenReturn(42d);
        var registry = new SimpleMeterRegistry();
        try (var snapshot = new OutboxMetricsSnapshot(dataSource, registry, Clock.fixed(Instant.EPOCH, ZoneOffset.UTC))) {
            snapshot.refresh();
            assertThat(registry.get("warexis.outbox.pending").gauge().value()).isEqualTo(10001);
            assertThat(registry.get("warexis.outbox.oldest.age").gauge().value()).isEqualTo(42);
            assertThat(registry.get("warexis.outbox.snapshot.available").gauge().value()).isEqualTo(1);
            verify(dataSource).getConnection();
            verify(statement).setQueryTimeout(1);
            verify(statement).executeQuery(contains("LIMIT 10001"));
            verify(statement).executeQuery(contains("WHERE published_at IS NULL"));
        }
    }

    @Test void emptySnapshotIsZeroAndFailedRefreshIsUnavailableRatherThanZero() throws Exception {
        var dataSource = mock(DataSource.class);
        var connection = mock(Connection.class);
        var statement = mock(Statement.class);
        var row = mock(ResultSet.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(row);
        when(row.next()).thenReturn(true);
        var registry = new SimpleMeterRegistry();
        try (var snapshot = new OutboxMetricsSnapshot(dataSource, registry, Clock.fixed(Instant.EPOCH, ZoneOffset.UTC))) {
            snapshot.refresh();
            assertThat(registry.get("warexis.outbox.pending").gauge().value()).isZero();
            assertThat(registry.get("warexis.outbox.oldest.age").gauge().value()).isZero();
            when(dataSource.getConnection()).thenThrow(new java.sql.SQLException("unavailable"));
            snapshot.refresh();
            assertThat(registry.get("warexis.outbox.pending").gauge().value()).isNaN();
            assertThat(registry.get("warexis.outbox.oldest.age").gauge().value()).isNaN();
            assertThat(registry.get("warexis.outbox.snapshot.available").gauge().value()).isZero();
        }
    }

    @Test void staleSnapshotIsUnavailableEvenWhenNextRefreshCannotAcquireConnection() throws Exception {
        var dataSource = mock(DataSource.class);
        var connection = mock(Connection.class);
        var statement = mock(Statement.class);
        var row = mock(ResultSet.class);
        var clock = mock(Clock.class);
        when(clock.millis()).thenReturn(0L);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(row);
        when(row.next()).thenReturn(true);
        try (var snapshot = new OutboxMetricsSnapshot(dataSource, new SimpleMeterRegistry(), clock)) {
            snapshot.refresh();
            when(clock.millis()).thenReturn(31_000L);
            when(dataSource.getConnection()).thenThrow(new java.sql.SQLException("unavailable"));
            assertThat(snapshot.current().available()).isFalse();
        }
    }

    @Test void scrapeDoesNotWaitForUnavailableBusinessPool() throws Exception {
        var dataSource = mock(DataSource.class);
        var release = new java.util.concurrent.CountDownLatch(1);
        when(dataSource.getConnection()).thenAnswer(invocation -> { release.await(); throw new java.sql.SQLException(); });
        try (var snapshot = new OutboxMetricsSnapshot(dataSource, new SimpleMeterRegistry(), Clock.systemUTC())) {
            org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(java.time.Duration.ofSeconds(1), () -> {
                assertThat(snapshot.current().pending()).isNaN();
                assertThat(snapshot.current().available()).isFalse();
            });
        } finally { release.countDown(); }
    }
}
