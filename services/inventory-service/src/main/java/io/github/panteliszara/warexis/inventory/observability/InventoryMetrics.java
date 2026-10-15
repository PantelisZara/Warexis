package io.github.panteliszara.warexis.inventory.observability;

import java.util.HashMap;
import java.util.Map;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/** Bounded, process-local operational observations. Telemetry must never alter delivery behavior. */
@Component
public class InventoryMetrics {
    private final Map<String, Counter> counters = new HashMap<>();

    @org.springframework.beans.factory.annotation.Autowired
    public InventoryMetrics(MeterRegistry registry) {
        for (var result : new String[]{"published", "failed"}) register(registry, "warexis.outbox.publications", "result", result);
        for (var result : new String[]{"processed", "duplicate", "ignored", "dlt"}) register(registry, "warexis.messaging.deliveries", "result", result);
        for (var stage : new String[]{"process", "dlt"}) {
            register(registry, "warexis.messaging.failures", "stage", stage);
            register(registry, "warexis.messaging.retries", "stage", stage);
        }
        register(registry, "warexis.outbox.retries", null, null);
        register(registry, "warexis.messaging.pauses", null, null);
    }

    private InventoryMetrics() {}

    public static InventoryMetrics disabled() { return new InventoryMetrics(); }

    private void register(MeterRegistry registry, String name, String label, String value) {
        try {
            var builder = Counter.builder(name);
            if (label != null) builder.tag(label, value);
            counters.put(name + ":" + value, builder.register(registry));
        } catch (RuntimeException ignored) { /* Optional instrumentation fails open. */ }
    }

    private void increment(String name, String value) {
        try {
            var counter = counters.get(name + ":" + value);
            if (counter != null) counter.increment();
        } catch (RuntimeException ignored) { /* A registry/exporter failure cannot fail a business operation. */ }
    }

    public void publication(boolean published) { increment("warexis.outbox.publications", published ? "published" : "failed"); }
    public void outboxRetry() { increment("warexis.outbox.retries", null); }
    public void delivery(String result) { increment("warexis.messaging.deliveries", result); }
    public void failure(String stage) { increment("warexis.messaging.failures", stage); }
    public void retry(String stage) { increment("warexis.messaging.retries", stage); }
    public void pause() { increment("warexis.messaging.pauses", null); }
}
