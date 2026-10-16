package io.github.panteliszara.warexis.inventory;

import io.github.panteliszara.warexis.inventory.observability.*;
import io.micrometer.tracing.Tracer;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"warexis.messaging.enabled=false", "management.tracing.sampling.probability=1.0",
                "WAREXIS_OAUTH2_ISSUER_URI=https://issuer.test/realms/warexis", "WAREXIS_OAUTH2_AUDIENCE=warexis-api"})
@Import({io.github.panteliszara.warexis.inventory.security.TestJwtConfiguration.class, InventoryTracingRuntimeTests.CaptureConfiguration.class})
@Testcontainers
class InventoryTracingRuntimeTests {
    static final String TP = "00-12345678901234567890123456789012-1234567890123456-01";
    static final List<SpanData> SPANS = new CopyOnWriteArrayList<>();
    @Container @ServiceConnection static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18.6");
    @Autowired Tracer tracer;
    @Autowired OpenTelemetry telemetry;
    @Autowired ApplicationContext context;
    @LocalServerPort int port;
    @BeforeEach void reset() { flush(); SPANS.clear(); }
    void flush() { context.getBean(SdkTracerProvider.class).forceFlush().join(3, TimeUnit.SECONDS); }
    List<SpanData> named(String name) { flush(); return SPANS.stream().filter(s -> s.getName().equals(name)).toList(); }

    @Test void nativeHttpUsesStrictRemoteContextAndWholeExporterPrivacy() throws Exception {
        try (var client = HttpClient.newHttpClient()) {
            var response = client.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/actuator/health?private=SENTINEL"))
                    .header("TraceParent", TP.substring(0, 53) + "ff")
                    .header("baggage", "secret=SENTINEL").header("tracestate", "private=SENTINEL").GET().build(), HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> assertThat(named("http.request")).hasSize(1));
            var http = named("http.request").getFirst();
            assertThat(http.getTraceId()).isEqualTo(TP.substring(3, 35));
            assertThat(http.getParentSpanId()).isEqualTo(TP.substring(36, 52));
            assertThat(http.getParentSpanContext().isRemote()).isTrue();
            assertSafe(http);
            SPANS.clear();
            client.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/actuator/health"))
                    .header("traceparent", TP).header("traceparent", TP).GET().build(), HttpResponse.BodyHandlers.ofString());
            await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> assertThat(named("http.request")).hasSize(1));
            assertThat(named("http.request").getFirst().getParentSpanId()).isEqualTo("0000000000000000");
        }
        var remote = SpanContext.createFromRemoteParent("22345678901234567890123456789012", "2234567890123456",
                TraceFlags.getSampled(), TraceState.builder().put("private", "SENTINEL").build());
        var raw = telemetry.getTracer("SENTINEL", "SENTINEL").spanBuilder("SENTINEL")
                .setNoParent().addLink(remote, Attributes.builder().put("private", "SENTINEL").build()).startSpan();
        raw.setAttribute("url.full", "https://private/SENTINEL").setAttribute("warexis.outcome", "SENTINEL");
        raw.setAttribute("http.response.status_code", 700L);
        raw.addEvent("SENTINEL"); raw.recordException(new IllegalStateException("SENTINEL"));
        raw.setStatus(StatusCode.ERROR, "SENTINEL"); raw.end(); flush();
        var safe = SPANS.stream().filter(s -> s.getSpanId().equals(raw.getSpanContext().getSpanId())).findFirst().orElseThrow();
        assertSafe(safe); assertThat(safe.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
        assertThat(safe.getLinks()).hasSize(1);
        assertThat(safe.getLinks().getFirst().getSpanContext().isRemote()).isTrue();
        assertThat(safe.getLinks().getFirst().getSpanContext().getTraceState()).isEqualTo(TraceState.getDefault());
        assertThat(safe.getLinks().getFirst().getAttributes()).isEqualTo(Attributes.empty());
        assertThat(context.getBeansOfType(SpanExporter.class).values()).allSatisfy(e -> assertThat(e).isInstanceOf(PrivacySpanExporter.class));
        assertThat(context.getBeanProvider(io.micrometer.registry.otlp.OtlpMeterRegistry.class).stream()).isEmpty();
        assertThat(context.getBeansOfType(io.opentelemetry.sdk.logs.export.LogRecordExporter.class)).isEmpty();
        assertThat(Baggage.current().asMap()).isEmpty();
    }

    @Test void setupFailureNeverCapturesUnrelatedAmbientContext() throws Exception {
        var outer = tracer.spanBuilder().setNoParent().name("outer").start();
        var broken = spy(tracer);
        doThrow(new IllegalStateException("telemetry setup")).when(broken).spanBuilder();
        var calls = new AtomicInteger();
        var failOpen = new TraceSupport(broken, context.getBean(io.micrometer.tracing.propagation.Propagator.class));
        try (var scope = tracer.withSpan(outer)) {
            String original = failOpen.currentTraceparent();
            failOpen.attempt(TraceSupport.Operation.PROCESS, "invalid", "unknown", () -> {
                calls.incrementAndGet();
                assertThat(failOpen.currentTraceparent()).isNull(); return null;
            });
            assertThat(failOpen.currentTraceparent()).isEqualTo(original);
            assertThat(tracer.currentSpan().context().spanId()).isEqualTo(outer.context().spanId());
            assertThat(calls).hasValue(1);
        } finally { outer.end(); }
    }

    static void assertSafe(SpanData span) {
        assertThat(span.toString()).doesNotContain("SENTINEL", "private", "SELECT", "/home/", "Bearer");
        assertThat(span.getEvents()).isEmpty(); assertThat(span.getStatus().getDescription()).isEmpty();
        assertThat(span.getResource().getAttributes()).isEqualTo(Attributes.builder().put("service.name", "inventory-service").build());
        assertThat(span.getResource().getSchemaUrl()).isNull();
        assertThat(span.getInstrumentationScopeInfo().getName()).isEqualTo("warexis.tracing");
        assertThat(span.getInstrumentationScopeInfo().getVersion()).isEqualTo("1");
        assertThat(span.getInstrumentationScopeInfo().getAttributes()).isEqualTo(Attributes.empty());
        assertThat(span.getInstrumentationScopeInfo().getSchemaUrl()).isNull();
        assertThat(span.getSpanContext().getTraceState()).isEqualTo(TraceState.getDefault());
        assertThat(span.getParentSpanContext().getTraceState()).isEqualTo(TraceState.getDefault());
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class CaptureConfiguration {
        @Bean SpanExporter captureExporter() {
            return new SpanExporter() {
                public CompletableResultCode export(Collection<SpanData> spans) { SPANS.addAll(spans); return CompletableResultCode.ofSuccess(); }
                public CompletableResultCode flush() { return CompletableResultCode.ofSuccess(); }
                public CompletableResultCode shutdown() { return CompletableResultCode.ofSuccess(); }
            };
        }
    }
}
