package io.github.panteliszara.warexis.inventory.observability;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.*;

class InventoryMetricsTests {
    @Test
    void boundedVariantsStartAtZeroAndUnknownValuesCannotCreateLabels() {
        var registry = new SimpleMeterRegistry();
        try {
            var metrics = new InventoryMetrics(registry);
            assertThat(registry.getMeters()).hasSize(12);
            assertThat(registry.getMeters()).allSatisfy(meter -> {
                assertThat(meter.getId().getTags()).hasSizeLessThanOrEqualTo(1);
                assertThat(meter.measure()).allSatisfy(measurement -> assertThat(measurement.getValue()).isZero());
            });
            metrics.delivery("untrusted-event-id");
            metrics.failure("raw exception");
            metrics.publication(true);
            metrics.publication(false);
            metrics.outboxRetry();
            metrics.retry("process");
            metrics.pause();
            assertThat(registry.getMeters()).hasSize(12);
            assertThat(registry.get("warexis.outbox.publications").tag("result", "published").counter().count()).isEqualTo(1);
            assertThat(registry.get("warexis.outbox.publications").tag("result", "failed").counter().count()).isEqualTo(1);
        } finally { registry.close(); }
    }

    @Test
    void counterRecordingFailureCannotEscapeIntoBusinessCode() {
        var registry = new SimpleMeterRegistry() {
            @Override
            protected Counter newCounter(Meter.Id id) {
                var counter = mock(Counter.class);
                when(counter.getId()).thenReturn(id);
                doThrow(new IllegalStateException("exporter unavailable")).when(counter).increment();
                return counter;
            }
        };
        try {
            var metrics = new InventoryMetrics(registry);
            assertThatCode(() -> {
                metrics.publication(true);
                metrics.delivery("processed");
                metrics.retry("process");
                metrics.pause();
            }).doesNotThrowAnyException();
        } finally { registry.close(); }
    }

    @Test
    void registryFailureAndDisabledMetricsCannotFailBusinessPath() {
        var registry = mock(io.micrometer.core.instrument.MeterRegistry.class);
        assertThatCode(() -> {
            var metrics = new InventoryMetrics(registry);
            metrics.publication(true);
            metrics.delivery("processed");
            metrics.pause();
        }).doesNotThrowAnyException();
        var disabled = new SimpleMeterRegistry();
        try {
            disabled.config().meterFilter(MeterFilter.deny());
            var metrics = new InventoryMetrics(disabled);
            metrics.publication(true);
            metrics.retry("dlt");
            assertThat(disabled.getMeters()).isEmpty();
        } finally { disabled.close(); }
        assertThatCode(() -> InventoryMetrics.disabled().delivery("processed")).doesNotThrowAnyException();
    }
}
