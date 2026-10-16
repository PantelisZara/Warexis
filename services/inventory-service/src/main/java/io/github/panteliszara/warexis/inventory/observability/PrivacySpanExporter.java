package io.github.panteliszara.warexis.inventory.observability;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.data.StatusData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/** Every managed exporter receives only immutable reconstructed finite data. */
public final class PrivacySpanExporter implements SpanExporter {
    private final SpanExporter delegate;
    public PrivacySpanExporter(SpanExporter delegate) { this.delegate = delegate; }
    @Override public CompletableResultCode export(Collection<SpanData> spans) {
        try { return delegate.export(spans.stream().map(SafeSpan::new).map(s -> (SpanData) s).toList()); }
        catch (Throwable ignored) { return CompletableResultCode.ofFailure(); }
    }
    @Override public CompletableResultCode flush() {
        try { return delegate.flush(); } catch (Throwable ignored) { return CompletableResultCode.ofFailure(); }
    }
    @Override public CompletableResultCode shutdown() {
        try { return delegate.shutdown(); } catch (Throwable ignored) { return CompletableResultCode.ofFailure(); }
    }

    record SafeSpan(String name, SpanKind kind, SpanContext spanContext, SpanContext parentSpanContext,
            StatusData status, long startEpochNanos, Attributes attributes, List<LinkData> links,
            long endEpochNanos, boolean ended) implements SpanData {
        SafeSpan(SpanData raw) {
            this(safeName(raw), raw.getKind(), clean(raw.getSpanContext()), clean(raw.getParentSpanContext()),
                    StatusData.create(raw.getStatus().getStatusCode() == StatusCode.ERROR ? StatusCode.ERROR : StatusCode.UNSET, ""),
                    raw.getStartEpochNanos(), safeAttributes(raw.getAttributes()),
                    raw.getLinks().stream().limit(1).map(l -> LinkData.create(clean(l.getSpanContext()), Attributes.empty())).toList(),
                    raw.getEndEpochNanos(), raw.hasEnded());
        }
        public String getName() { return name; }
        public SpanKind getKind() { return kind; }
        public SpanContext getSpanContext() { return spanContext; }
        public SpanContext getParentSpanContext() { return parentSpanContext; }
        public StatusData getStatus() { return status; }
        public long getStartEpochNanos() { return startEpochNanos; }
        public Attributes getAttributes() { return attributes; }
        public List<EventData> getEvents() { return List.of(); }
        public List<LinkData> getLinks() { return links; }
        public long getEndEpochNanos() { return endEpochNanos; }
        public boolean hasEnded() { return ended; }
        public int getTotalRecordedEvents() { return 0; }
        public int getTotalRecordedLinks() { return links.size(); }
        public int getTotalAttributeCount() { return attributes.size(); }
        public Resource getResource() { return Resource.create(Attributes.builder().put("service.name", "inventory-service").build()); }
        @SuppressWarnings("deprecation")
        public InstrumentationLibraryInfo getInstrumentationLibraryInfo() { return InstrumentationLibraryInfo.create("warexis.tracing", "1"); }
        public InstrumentationScopeInfo getInstrumentationScopeInfo() { return InstrumentationScopeInfo.create("warexis.tracing", "1", null); }
    }

    static SpanContext clean(SpanContext context) {
        if (!context.isValid()) return SpanContext.getInvalid();
        var flags = context.isSampled() ? TraceFlags.getSampled() : TraceFlags.getDefault();
        return context.isRemote()
                ? SpanContext.createFromRemoteParent(context.getTraceId(), context.getSpanId(), flags, TraceState.getDefault())
                : SpanContext.create(context.getTraceId(), context.getSpanId(), flags, TraceState.getDefault());
    }
    private static String safeName(SpanData span) {
        if (span.getKind() == SpanKind.SERVER) return "http.request";
        return Set.of("http.request", "warexis.outbox.publish", "warexis.kafka.process", "warexis.kafka.dlt",
                "warexis.fulfillment.dispatch").contains(span.getName()) ? span.getName() : "warexis.operation";
    }
    private static Attributes safeAttributes(Attributes raw) {
        var out = Attributes.builder();
        raw.forEach((key, value) -> {
            String name = key.getKey();
            if (value instanceof String text) {
                if (name.equals("warexis.operation") && Set.of("outbox.publish", "kafka.process", "kafka.dlt", "fulfillment.dispatch").contains(text)) out.put(name, text);
                if (name.equals("warexis.outcome") && Set.of("success", "failure").contains(text)) out.put(name, text);
                if (name.equals("warexis.event_type") && Set.of("OrderPlaced", "InventoryReserved", "InventoryRejected", "OrderConfirmed", "FulfillmentShipped", "unknown").contains(text)) out.put(name, text);
                if ((name.equals("http.method") || name.equals("http.request.method"))
                        && Set.of("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "CONNECT", "TRACE").contains(text)) out.put("http.request.method", text);
            }
            if ((name.equals("http.status_code") || name.equals("http.response.status_code"))
                    && value instanceof Long code && code >= 100 && code <= 599) out.put("http.response.status_code", code);
        });
        return out.build();
    }
}
