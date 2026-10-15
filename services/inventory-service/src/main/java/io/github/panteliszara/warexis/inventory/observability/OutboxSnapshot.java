package io.github.panteliszara.warexis.inventory.observability;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/** Indexed, capped snapshots sampled independently of business scheduling and scrape threads. */
@Component
public class OutboxSnapshot {
    private record Snapshot(double pending, double age, Instant sampledAt) {}
    private final DataSource dataSource;
    private final Clock clock;
    private final ScheduledExecutorService sampler;
    private volatile Snapshot snapshot;

    @Autowired
    public OutboxSnapshot(DataSource dataSource, MeterRegistry registry) {
        this(dataSource, registry, Clock.systemUTC(), true);
    }

    OutboxSnapshot(DataSource dataSource, MeterRegistry registry, Clock clock, boolean start) {
        this.dataSource = dataSource;
        this.clock = clock;
        for (var kind : new String[]{"pending", "oldest.age", "snapshot.available"}) {
            try {
                var builder = Gauge.builder("warexis.outbox." + kind, this, value -> value.value(kind));
                if (kind.equals("oldest.age")) builder.baseUnit("seconds");
                builder.register(registry);
            } catch (RuntimeException ignored) { /* Registry disablement cannot prevent startup. */ }
        }
        sampler = Executors.newSingleThreadScheduledExecutor(task -> {
            var thread = new Thread(task, "inventory-outbox-metrics");
            thread.setDaemon(true);
            return thread;
        });
        if (start) sampler.scheduleWithFixedDelay(this::refresh, 0, 15, TimeUnit.SECONDS);
    }

    void refresh() {
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement("""
                SELECT count(*), coalesce(greatest(0, extract(epoch FROM CURRENT_TIMESTAMP - min(occurred_at))), 0)
                FROM (SELECT occurred_at FROM outbox_events WHERE published_at IS NULL
                      ORDER BY occurred_at, event_id LIMIT 10001) pending
                """)) {
            statement.setQueryTimeout(2);
            try (var row = statement.executeQuery()) {
                if (!row.next()) throw new IllegalStateException("Snapshot unavailable.");
                snapshot = new Snapshot(row.getDouble(1), row.getDouble(2), clock.instant());
            }
        } catch (Exception ignored) { snapshot = null; }
    }

    private double value(String kind) {
        var current = snapshot;
        var available = current != null && Duration.between(current.sampledAt(), clock.instant()).compareTo(Duration.ofSeconds(30)) < 0;
        if (kind.equals("snapshot.available")) return available ? 1 : 0;
        if (!available) return Double.NaN;
        return kind.equals("pending") ? current.pending() : current.age();
    }

    @PreDestroy
    void stop() { sampler.shutdownNow(); }
}
