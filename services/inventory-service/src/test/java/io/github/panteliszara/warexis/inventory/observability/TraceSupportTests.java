package io.github.panteliszara.warexis.inventory.observability;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class TraceSupportTests {
    @ParameterizedTest
    @ValueSource(strings = {"builder", "start", "tag", "scope", "status", "error", "close", "end", "none"})
    void telemetryFailuresNeverReinvokeOrReplaceBusiness(String fault) throws Exception {
        var tracer = mock(Tracer.class);
        var builder = mock(Span.Builder.class, RETURNS_SELF);
        var span = mock(Span.class, RETURNS_SELF);
        var scope = mock(Tracer.SpanInScope.class);
        when(tracer.spanBuilder()).thenReturn(builder);
        when(builder.start()).thenReturn(span);
        when(tracer.withSpan(span)).thenReturn(scope);
        var telemetry = new IllegalStateException("private telemetry failure");
        switch (fault) {
            case "builder" -> when(tracer.spanBuilder()).thenThrow(telemetry);
            case "start" -> when(builder.start()).thenThrow(telemetry);
            case "tag" -> when(span.tag(eq("warexis.operation"), anyString())).thenThrow(telemetry);
            case "scope" -> when(tracer.withSpan(span)).thenThrow(telemetry);
            case "status" -> when(span.tag(eq("warexis.outcome"), anyString())).thenThrow(telemetry);
            case "error" -> when(span.error(any())).thenThrow(telemetry);
            case "close" -> doThrow(telemetry).when(scope).close();
            case "end" -> doThrow(telemetry).when(span).end();
            default -> { }
        }
        var support = new TraceSupport(tracer, Propagator.NOOP);
        var calls = new AtomicInteger();
        assertThat(support.attempt(TraceSupport.Operation.PUBLISH, null, "OrderPlaced", () -> {
            calls.incrementAndGet(); return "business";
        })).isEqualTo("business");
        assertThat(calls).hasValue(1);
        calls.set(0);
        var original = new InterruptedException("private business failure");
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> support.attempt(TraceSupport.Operation.PROCESS, null, "unknown", () -> {
                calls.incrementAndGet(); throw original;
            })).isSameAs(original);
            assertThat(calls).hasValue(1);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally { Thread.interrupted(); }
        var fatal = new AssertionError("business error");
        assertThatThrownBy(() -> support.attempt(TraceSupport.Operation.DLT, null, "unknown", () -> { throw fatal; })).isSameAs(fatal);
        assertThatThrownBy(() -> support.dispatch(null, () -> { throw fatal; })).isSameAs(fatal);
    }
}
