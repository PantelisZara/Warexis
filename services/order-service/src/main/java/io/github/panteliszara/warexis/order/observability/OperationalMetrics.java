package io.github.panteliszara.warexis.order.observability;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

/** Process-local attempt/completion observations; never a durable business ledger. */
@Component
public class OperationalMetrics {
    private final MeterRegistry registry;

    @org.springframework.beans.factory.annotation.Autowired
    public OperationalMetrics(ObjectProvider<MeterRegistry> registries) {
        this(registries.getIfAvailable(), true);
    }

    public OperationalMetrics(MeterRegistry registry, boolean initialize) {
        this.registry = registry;
        if (initialize) {
            for (String result : new String[]{"published", "failed"}) counter("warexis.outbox.publications", "result", result, false);
            for (String result : new String[]{"processed", "duplicate", "ignored", "dlt"}) counter("warexis.messaging.deliveries", "result", result, false);
            for (String stage : new String[]{"process", "dlt"}) {
                counter("warexis.messaging.failures", "stage", stage, false);
                counter("warexis.messaging.retries", "stage", stage, false);
            }
            counter("warexis.outbox.retries", null, null, false);
            counter("warexis.messaging.pauses", null, null, false);
        }
    }

    public void published() { committedCounter("warexis.outbox.publications", "result", "published", true); }
    public void publicationFailed() { counter("warexis.outbox.publications", "result", "failed", true); }
    public void publicationRetried() { counter("warexis.outbox.retries", null, null, true); }
    public void processed(boolean newDecision) { committedCounter("warexis.messaging.deliveries", "result", newDecision ? "processed" : "duplicate", true); }
    public void duplicate() { committedCounter("warexis.messaging.deliveries", "result", "duplicate", true); }
    public void dlt() { committedCounter("warexis.messaging.deliveries", "result", "dlt", true); }
    public void processFailed() { counter("warexis.messaging.failures", "stage", "process", true); }
    public void dltFailed() { counter("warexis.messaging.failures", "stage", "dlt", true); }
    public void processRetried() { counter("warexis.messaging.retries", "stage", "process", true); }
    public void dltRetried() { counter("warexis.messaging.retries", "stage", "dlt", true); }
    public void paused() { counter("warexis.messaging.pauses", null, null, true); }

    private void committedCounter(String name, String key, String value, boolean increment) {
        if (registry == null) return;
        try {
            if (org.springframework.transaction.support.TransactionSynchronizationManager.isActualTransactionActive()
                    && org.springframework.transaction.support.TransactionSynchronizationManager.isSynchronizationActive()) {
                org.springframework.transaction.support.TransactionSynchronizationManager.registerSynchronization(
                        new org.springframework.transaction.support.TransactionSynchronization() {
                            @Override public void afterCommit() { counter(name, key, value, increment); }
                        });
            } else {
                counter(name, key, value, increment);
            }
        } catch (RuntimeException ignored) {
            // A telemetry callback registration failure must not affect the ambient transaction.
        }
    }

    private void counter(String name, String key, String value, boolean increment) {
        if (registry == null) return;
        try {
            var meter = key == null ? registry.counter(name) : registry.counter(name, key, value);
            if (increment) meter.increment();
        } catch (RuntimeException ignored) {
            // Observability must never change transaction, retry or acknowledgment behavior.
        }
    }
}
