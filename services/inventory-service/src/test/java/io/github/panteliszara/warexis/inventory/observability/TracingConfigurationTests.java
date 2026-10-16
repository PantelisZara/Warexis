package io.github.panteliszara.warexis.inventory.observability;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class TracingConfigurationTests {
    @Test void throwingTransportGetterStartsRootWithoutAmbientFallback() {
        var remote = SpanContext.createFromRemoteParent("12345678901234567890123456789012", "1234567890123456",
                TraceFlags.getSampled(), TraceState.getDefault());
        var ambient = Context.root().with(Span.wrap(remote));
        var strict = new TracingConfiguration.StrictTraceparent();
        var extracted = strict.extract(ambient, "ignored", new TextMapGetter<String>() {
            public Iterable<String> keys(String carrier) { return List.of("traceparent"); }
            public String get(String carrier, String key) { throw new IllegalStateException("transport unavailable"); }
        });
        assertThat(Span.fromContext(extracted).getSpanContext().isValid()).isFalse();
    }
}
