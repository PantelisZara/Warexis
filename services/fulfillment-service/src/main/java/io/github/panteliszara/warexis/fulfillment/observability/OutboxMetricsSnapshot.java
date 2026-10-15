package io.github.panteliszara.warexis.fulfillment.observability;

import java.sql.SQLException;
import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import jakarta.annotation.PreDestroy;

/** Bounded indexed read on a dedicated telemetry thread, never a business scheduler or scrape thread. */
@Component
public class OutboxMetricsSnapshot implements AutoCloseable {
    static final long REFRESH_MILLIS = 15_000;
    static final long STALE_MILLIS = 30_000;
    private final DataSource dataSource;
    private final Clock clock;
    private final ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
        var thread = new Thread(runnable, "fulfillment-outbox-metrics");
        thread.setDaemon(true);
        return thread;
    });
    private final AtomicBoolean refreshing = new AtomicBoolean();
    private volatile Snapshot snapshot = new Snapshot(Double.NaN, Double.NaN, 0, false);
    private volatile long lastAttempt = Long.MIN_VALUE;

    @Autowired
    public OutboxMetricsSnapshot(DataSource dataSource, ObjectProvider<MeterRegistry> registries) {
        this(dataSource, registries.getIfAvailable(), Clock.systemUTC());
    }

    OutboxMetricsSnapshot(DataSource dataSource, MeterRegistry registry, Clock clock) {
        this.dataSource = dataSource;
        this.clock = clock;
        if (registry != null) {
            try {
                Gauge.builder("warexis.outbox.pending", this, value -> value.current().pending()).register(registry);
                Gauge.builder("warexis.outbox.oldest.age", this, value -> value.current().age()).baseUnit("seconds").register(registry);
                Gauge.builder("warexis.outbox.snapshot.available", this, value -> value.current().available() ? 1 : 0).register(registry);
            } catch (RuntimeException ignored) {
                // A disabled/failed registry cannot prevent service startup.
            }
        }
    }

    Snapshot current() {
        long now = clock.millis();
        if ((lastAttempt == Long.MIN_VALUE || now - lastAttempt >= REFRESH_MILLIS)
                && refreshing.compareAndSet(false, true)) {
            lastAttempt = now;
            try {
                executor.execute(this::refresh);
            } catch (RuntimeException ignored) {
                refreshing.set(false);
            }
        }
        var value = snapshot;
        return value.available() && now - value.at() <= STALE_MILLIS
                ? value : new Snapshot(Double.NaN, Double.NaN, value.at(), false);
    }

    void refresh() {
        lastAttempt = clock.millis();
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.setQueryTimeout(1);
            try (var row = statement.executeQuery("""
                    SELECT count(*), COALESCE(GREATEST(0, EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - min(occurred_at)))), 0)
                    FROM (SELECT occurred_at FROM outbox_events WHERE published_at IS NULL
                          ORDER BY occurred_at, event_id LIMIT 10001) pending
                    """)) {
                if (!row.next()) throw new SQLException("Missing outbox snapshot");
                snapshot = new Snapshot(row.getDouble(1), row.getDouble(2), clock.millis(), true);
            }
        } catch (Exception ignored) {
            snapshot = new Snapshot(Double.NaN, Double.NaN, clock.millis(), false);
        } finally {
            refreshing.set(false);
        }
    }

    @PreDestroy
    public void close() { executor.shutdownNow(); }

    record Snapshot(double pending, double age, long at, boolean available) { }
}
