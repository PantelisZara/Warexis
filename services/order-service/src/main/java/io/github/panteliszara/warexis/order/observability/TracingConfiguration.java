package io.github.panteliszara.warexis.order.observability;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.util.Collection;
import java.util.List;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class TracingConfiguration {
    @Bean
    ContextPropagators strictContextPropagators() {
        return ContextPropagators.create(new StrictTraceparent());
    }

    @Bean
    static BeanPostProcessor privacyExporters() {
        return new BeanPostProcessor() {
            @Override
            public Object postProcessAfterInitialization(Object bean, String name) {
                return bean instanceof SpanExporter exporter ? new PrivacySpanExporter(exporter) : bean;
            }
        };
    }

    /** The native HTTP owner uses exactly the same strict policy as durable/Kafka context. */
    static final class StrictTraceparent implements TextMapPropagator {
        @Override public Collection<String> fields() { return List.of("traceparent"); }
        @Override public <C> void inject(Context context, C carrier, TextMapSetter<C> setter) {
            var span = Span.fromContext(context).getSpanContext();
            if (span.isValid()) setter.set(carrier, "traceparent", "00-" + span.getTraceId() + "-"
                    + span.getSpanId() + (span.isSampled() ? "-01" : "-00"));
        }
        @Override public <C> Context extract(Context ignored, C carrier, TextMapGetter<C> getter) {
            try {
                var values = getter.getAll(carrier, "traceparent");
                if (values == null || !values.hasNext()) return Context.root();
                String parent = TraceParent.normalize(values.next());
                if (values.hasNext() || parent == null) return Context.root();
                var context = SpanContext.createFromRemoteParent(parent.substring(3, 35), parent.substring(36, 52),
                        parent.endsWith("01") ? TraceFlags.getSampled() : TraceFlags.getDefault(), TraceState.getDefault());
                return Context.root().with(Span.wrap(context));
            } catch (RuntimeException ignoredFailure) { return Context.root(); }
        }
    }
}
