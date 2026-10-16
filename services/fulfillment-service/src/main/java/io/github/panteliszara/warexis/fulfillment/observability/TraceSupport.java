package io.github.panteliszara.warexis.fulfillment.observability;

import io.micrometer.tracing.Link;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.springframework.stereotype.Component;

/** Service-local attempts; telemetry must never retry or replace business work. */
@Component
public class TraceSupport {
    public enum Operation {
        PUBLISH("outbox.publish", Span.Kind.PRODUCER),
        PROCESS("kafka.process", Span.Kind.CONSUMER),
        DLT("kafka.dlt", Span.Kind.PRODUCER);
        final String value;
        final Span.Kind kind;
        Operation(String value, Span.Kind kind) { this.value = value; this.kind = kind; }
    }

    @FunctionalInterface
    public interface CheckedSupplier<T> { T get() throws Exception; }

    private static final Set<String> EVENTS = Set.of("OrderPlaced", "InventoryReserved", "InventoryRejected",
            "OrderConfirmed", "FulfillmentShipped", "unknown");
    private final ThreadLocal<Boolean> captureSuppressed = new ThreadLocal<>();
    private final Tracer tracer;
    private final Propagator propagator;

    public TraceSupport(Tracer tracer, Propagator propagator) {
        this.tracer = tracer;
        this.propagator = propagator;
    }

    public static TraceSupport noop() { return new TraceSupport(Tracer.NOOP, Propagator.NOOP); }

    public String currentTraceparent() {
        if (Boolean.TRUE.equals(captureSuppressed.get())) return null;
        try {
            var span = tracer.currentSpan();
            if (span == null) return null;
            var context = span.context();
            return TraceParent.normalize("00-" + context.traceId() + "-" + context.spanId()
                    + (Boolean.TRUE.equals(context.sampled()) ? "-01" : "-00"));
        } catch (Throwable ignored) { return null; }
    }

    public <T> T attempt(Operation operation, String explicitParent, String eventType,
            CheckedSupplier<T> body) throws Exception {
        Span span = null;
        Tracer.SpanInScope scope = null;
        try {
            String parent = TraceParent.normalize(explicitParent);
            var builder = parent == null ? tracer.spanBuilder().setNoParent()
                    : propagator.extract(Map.of("traceparent", parent), Map::get);
            span = builder.name("warexis." + operation.value).kind(operation.kind).start();
            scope = tracer.withSpan(span);
            span.tag("warexis.operation", operation.value);
            span.tag("warexis.event_type", eventType != null && EVENTS.contains(eventType) ? eventType : "unknown");
        } catch (Throwable ignored) { /* Partial setup is still finished below. */ }
        Boolean previousSuppression = captureSuppressed.get();
        captureSuppressed.set(scope == null);
        try {
            T result = body.get();
            outcome(span, false);
            return result;
        } catch (Exception | Error failure) {
            outcome(span, true);
            throw failure;
        } finally {
            close(scope);
            end(span);
            if (previousSuppression == null) captureSuppressed.remove();
            else captureSuppressed.set(previousSuppression);
        }
    }

    public <T> T dispatch(String origin, Supplier<T> body) {
        Span span = null;
        Tracer.SpanInScope scope = null;
        try {
            var builder = tracer.spanBuilder().name("warexis.fulfillment.dispatch");
            String link = TraceParent.normalize(origin);
            if (link != null) {
                var context = tracer.traceContextBuilder().traceId(link.substring(3, 35))
                        .spanId(link.substring(36, 52)).sampled(link.endsWith("01")).build();
                builder.addLink(new Link(context));
            }
            span = builder.start();
            scope = tracer.withSpan(span);
            span.tag("warexis.operation", "fulfillment.dispatch");
        } catch (Throwable ignored) { }
        Boolean previousSuppression = captureSuppressed.get();
        captureSuppressed.set(scope == null);
        try {
            T result = body.get();
            outcome(span, false);
            return result;
        } catch (RuntimeException | Error failure) {
            outcome(span, true);
            throw failure;
        } finally {
            close(scope);
            end(span);
            if (previousSuppression == null) captureSuppressed.remove();
            else captureSuppressed.set(previousSuppression);
        }
    }

    private static void outcome(Span span, boolean failed) {
        if (span == null) return;
        try { span.tag("warexis.outcome", failed ? "failure" : "success"); } catch (Throwable ignored) { }
        // No original exception content enters telemetry, even before export sanitization.
        if (failed) try { span.error(new AttemptFailed()); } catch (Throwable ignored) { }
    }
    private static void close(Tracer.SpanInScope scope) {
        if (scope != null) try { scope.close(); } catch (Throwable ignored) { }
    }
    private static void end(Span span) {
        if (span != null) try { span.end(); } catch (Throwable ignored) { }
    }
    private static final class AttemptFailed extends RuntimeException {
        AttemptFailed() { super(null, null, false, false); }
    }
}
