package io.github.panteliszara.warexis.order;

import io.github.panteliszara.warexis.order.messaging.*;
import io.github.panteliszara.warexis.order.observability.*;
import io.github.panteliszara.warexis.order.orders.application.OrderService;
import io.github.panteliszara.warexis.order.orders.domain.OrderLineInput;
import io.micrometer.tracing.Tracer;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"warexis.messaging.enabled=false", "management.tracing.sampling.probability=1.0"})
@Import({OrderServiceApplicationTests.PostgresConfiguration.class, TracingIntegrationTests.CaptureConfiguration.class})
class TracingIntegrationTests {
    static final String TP = "00-12345678901234567890123456789012-1234567890123456-01";
    static final List<SpanData> SPANS = new CopyOnWriteArrayList<>();
    @Autowired TraceSupport tracing;
    @Autowired Tracer tracer;
    @Autowired OpenTelemetry telemetry;
    @Autowired JdbcClient jdbc;
    @Autowired InventoryOutcomeService outcomes;
    @Autowired tools.jackson.databind.ObjectMapper mapper;
    @Autowired OrderService orders;
    @Autowired DataSource dataSource;
    @Autowired PlatformTransactionManager transactions;
    @Autowired ApplicationContext context;
    @LocalServerPort int port;

    @BeforeEach void reset() { flush(); SPANS.clear(); }
    void flush() { assertThat(context.getBean(SdkTracerProvider.class).forceFlush().join(3, TimeUnit.SECONDS).isSuccess()).isTrue(); }
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

    @Test void rootsExplicitParentsUnsampledDispatchLinksAndScopesUseActualManagedBridge() throws Exception {
        var outer = tracer.spanBuilder().setNoParent().name("private outer").start();
        try (var scope = tracer.withSpan(outer)) {
            String current = tracing.currentTraceparent();
            tracing.attempt(TraceSupport.Operation.PUBLISH, TP, "OrderPlaced", () -> {
                assertThat(tracing.currentTraceparent()).startsWith(TP.substring(0, 36)); return null;
            });
            tracing.attempt(TraceSupport.Operation.PROCESS, null, "bad value", () -> null);
            tracing.attempt(TraceSupport.Operation.PROCESS, "malformed", "unknown", () -> null);
            tracing.dispatch(TP, () -> { assertThat(tracing.currentTraceparent()).startsWith(current.substring(0, 36)); return null; });
            assertThat(tracing.currentTraceparent()).isEqualTo(current);
            tracing.attempt(TraceSupport.Operation.PROCESS, TP.substring(0, 53) + "00", "unknown", () -> {
                assertThat(tracing.currentTraceparent()).startsWith(TP.substring(0, 36)).endsWith("-00");
                var created = orders.create(List.of(new OrderLineInput("unsampled", 1)));
                assertThat(jdbc.sql("SELECT traceparent FROM outbox_events WHERE aggregate_id=:id").param("id", created.id()).query(String.class).single())
                        .isEqualTo(tracing.currentTraceparent());
                return null;
            });
            assertThat(tracing.currentTraceparent()).isEqualTo(current);
        } finally { outer.end(); }
        var publish = named("warexis.outbox.publish").getFirst();
        assertThat(publish.getParentSpanId()).isEqualTo(TP.substring(36, 52));
        assertThat(publish.getParentSpanContext().isRemote()).isTrue();
        assertThat(publish.getStatus().getStatusCode()).isEqualTo(StatusCode.UNSET);
        assertThat(publish.getAttributes().get(AttributeKey.stringKey("warexis.outcome"))).isEqualTo("success");
        assertThat(named("warexis.kafka.process")).hasSize(2).allSatisfy(s -> assertThat(s.getParentSpanId()).isEqualTo("0000000000000000"));
        var dispatch = named("warexis.fulfillment.dispatch").getFirst();
        assertThat(dispatch.getTraceId()).isEqualTo(outer.context().traceId());
        assertThat(dispatch.getParentSpanId()).isEqualTo(outer.context().spanId());
        assertThat(dispatch.getLinks().getFirst().getSpanContext().getTraceId()).isEqualTo(TP.substring(3, 35));
        assertThat(tracing.currentTraceparent()).isNull();
    }

    @Test void setupFailureNeverCapturesUnrelatedAmbientContext() throws Exception {
        var outer = tracer.spanBuilder().setNoParent().name("outer").start();
        var broken = spy(tracer);
        doThrow(new IllegalStateException("telemetry setup")).when(broken).spanBuilder();
        var failOpen = new TraceSupport(broken, context.getBean(io.micrometer.tracing.propagation.Propagator.class));
        try (var scope = tracer.withSpan(outer)) {
            String original = failOpen.currentTraceparent();
            failOpen.attempt(TraceSupport.Operation.PROCESS, "invalid", "unknown", () -> {
                assertThat(failOpen.currentTraceparent()).isNull(); return null;
            });
            assertThat(failOpen.currentTraceparent()).isEqualTo(original);
        } finally { outer.end(); }
    }

    @Test void actualProxyCommitFailureEndsAttemptAsFailureAndRollsBackOutbox() throws Exception {
        jdbc.sql("CREATE FUNCTION tracing_reject_commit() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RAISE EXCEPTION ''SENTINEL deferred SQL failure''; RETURN NEW; END'").update();
        jdbc.sql("CREATE CONSTRAINT TRIGGER tracing_reject_commit AFTER INSERT ON outbox_events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION tracing_reject_commit()").update();
        long before = jdbc.sql("SELECT count(*) FROM orders").query(Long.class).single();
        var calls = new AtomicInteger();
        try {
            assertThatThrownBy(() -> tracing.attempt(TraceSupport.Operation.PROCESS, TP, "InventoryReserved", () -> {
                calls.incrementAndGet(); return orders.create(List.of(new OrderLineInput("failure", 1)));
            })).isInstanceOf(org.springframework.orm.jpa.JpaSystemException.class);
            assertThat(calls).hasValue(1);
            assertThat(jdbc.sql("SELECT count(*) FROM orders").query(Long.class).single()).isEqualTo(before);
            var span = named("warexis.kafka.process").getFirst();
            assertThat(span.getAttributes().get(AttributeKey.stringKey("warexis.outcome"))).isEqualTo("failure");
            assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR); assertSafe(span);
        } finally {
            jdbc.sql("DROP TRIGGER tracing_reject_commit ON outbox_events").update();
            jdbc.sql("DROP FUNCTION tracing_reject_commit()").update();
        }
    }

    @Test @SuppressWarnings("unchecked")
    void relayAttemptIncludesAcknowledgmentAndDurablePublicationPreservingJsonAndKey() throws Exception {
        jdbc.sql("UPDATE outbox_events SET published_at=GREATEST(CURRENT_TIMESTAMP,occurred_at) WHERE published_at IS NULL").update();
        var created = tracing.attempt(TraceSupport.Operation.PROCESS, TP, "unknown", () -> orders.create(List.of(new OrderLineInput("relay", 1))));
        var origin = jdbc.sql("SELECT traceparent FROM outbox_events WHERE aggregate_id=:id").param("id", created.id()).query(String.class).single();
        var body = jdbc.sql("SELECT event_json::text FROM outbox_events WHERE aggregate_id=:id").param("id", created.id()).query(String.class).single();
        var kafka = (KafkaTemplate<byte[], byte[]>) mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class))).thenAnswer(invocation -> {
            ProducerRecord<byte[], byte[]> record = invocation.getArgument(0);
            assertThat(record.key()).isEqualTo(created.id().toString().getBytes(StandardCharsets.UTF_8));
            assertThat(record.value()).isEqualTo(body.getBytes(StandardCharsets.UTF_8));
            assertThat(record.topic()).isEqualTo(KafkaConfiguration.ORDER_TOPIC);
            assertThat(TraceParent.fromHeaders(record.headers())).isEqualTo(tracing.currentTraceparent());
            assertThat(named("warexis.outbox.publish")).isEmpty();
            return CompletableFuture.completedFuture(null);
        });
        new OutboxRelay(dataSource, jdbc, kafka, new MessagingProperties(false, Duration.ofMillis(1), Duration.ofMillis(10)), transactions,
                new OperationalMetrics(null, false), tracing).publishNext();
        var span = named("warexis.outbox.publish").getFirst();
        assertThat(span.getParentSpanId()).isEqualTo(origin.substring(36, 52));
        assertThat(span.getAttributes().get(AttributeKey.stringKey("warexis.outcome"))).isEqualTo("success");
        assertThat(jdbc.sql("SELECT published_at IS NOT NULL FROM outbox_events WHERE aggregate_id=:id").param("id", created.id()).query(Boolean.class).single()).isTrue();
        verify(kafka, times(1)).send(any(ProducerRecord.class));
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"success", "commit-failure", "completion-failure", "ack-failure"})
    @SuppressWarnings("unchecked")
    void processSpanIncludesActualValidatorAndServiceProxyCommitOnly(String mode) {
        var created = orders.create(List.of(new OrderLineInput("consumer", 1)));
        var event = mapper.createObjectNode();
        event.put("eventId", UUID.randomUUID().toString()); event.put("eventType", "InventoryReserved"); event.put("eventVersion", 1);
        event.put("occurredAt", java.time.Instant.now().toString());
        String reservation = UUID.randomUUID().toString();
        event.put("aggregateId", reservation); event.put("correlationId", created.id().toString());
        event.put("causationId", jdbc.sql("SELECT event_id FROM outbox_events WHERE aggregate_id=:id").param("id", created.id()).query(UUID.class).single().toString());
        var payload = event.putObject("payload"); payload.put("orderId", created.id().toString()); payload.put("reservationId", reservation);
        var lines = payload.putArray("lines");
        for (var line : created.lines()) {
            var node = lines.addObject(); node.put("orderLineId", line.id().toString()); node.put("sku", line.sku()); node.put("quantity", line.quantity());
        }
        var record = new org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]>(KafkaConfiguration.INVENTORY_TOPIC, 0, 123,
                created.id().toString().getBytes(StandardCharsets.UTF_8), mapper.writeValueAsBytes(event));
        record.headers().add("traceparent", TP.getBytes(StandardCharsets.US_ASCII));
        var deliveries = mock(DeliveryState.class);
        when(deliveries.load(record)).thenReturn(new DeliveryState.State("PROCESS", 0, 0, null, null),
                new DeliveryState.State(mode.equals("commit-failure") ? "PAUSED" : "COMPLETE", 1, 0, null, null));
        when(deliveries.begin(eq(record), any())).thenReturn(true);
        var ack = mock(org.springframework.kafka.support.Acknowledgment.class);
        if (mode.equals("ack-failure")) doThrow(new IllegalStateException("ack unavailable")).when(ack).acknowledge();
        doAnswer(call -> {
            var span = named("warexis.kafka.process").getFirst();
            assertThat(span.getAttributes().get(AttributeKey.stringKey("warexis.outcome"))).isEqualTo("success");
            assertThat(jdbc.sql("SELECT count(*) FROM outbox_events WHERE aggregate_id=:id AND event_type='OrderConfirmed'")
                    .param("id", created.id()).query(Long.class).single()).isEqualTo(1);
            if (mode.equals("completion-failure")) throw new IllegalStateException("completion metadata unavailable");
            return null;
        }).when(deliveries).complete(record);
        if (mode.equals("commit-failure")) {
            jdbc.sql("CREATE FUNCTION tracing_reject_process_commit() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RAISE EXCEPTION ''deferred process failure''; RETURN NEW; END'").update();
            jdbc.sql("CREATE CONSTRAINT TRIGGER tracing_reject_process_commit AFTER INSERT ON processed_inventory_events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION tracing_reject_process_commit()").update();
        }
        try {
            new InventoryOutcomeListener(outcomes, deliveries, mock(KafkaTemplate.class), mock(org.springframework.kafka.config.KafkaListenerEndpointRegistry.class),
                    new OperationalMetrics(null, false), tracing).receive(record, ack, mock(org.apache.kafka.clients.consumer.Consumer.class));
            var spans = named("warexis.kafka.process"); assertThat(spans).hasSize(1);
            assertThat(spans.getFirst().getParentSpanId()).isEqualTo(TP.substring(36, 52));
            assertThat(spans.getFirst().getAttributes().get(AttributeKey.stringKey("warexis.outcome")))
                    .isEqualTo(mode.equals("commit-failure") ? "failure" : "success");
            assertThat(jdbc.sql("SELECT count(*) FROM outbox_events WHERE aggregate_id=:id AND event_type='OrderConfirmed'")
                    .param("id", created.id()).query(Long.class).single()).isEqualTo(mode.equals("commit-failure") ? 0 : 1);
            if (mode.equals("success") || mode.equals("ack-failure")) verify(ack).acknowledge(); else verifyNoInteractions(ack);
        } finally {
            if (mode.equals("commit-failure")) {
                jdbc.sql("DROP TRIGGER tracing_reject_process_commit ON processed_inventory_events").update();
                jdbc.sql("DROP FUNCTION tracing_reject_process_commit()").update();
            }
        }
    }

    @Test @SuppressWarnings("unchecked")
    void dltSpanEndsAtBrokerAckBeforeCompletionFailureWithExactlyNineSafeHeaders() {
        var key = "key".getBytes(StandardCharsets.UTF_8); var body = "{".getBytes(StandardCharsets.UTF_8);
        var record = new org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]>(KafkaConfiguration.INVENTORY_TOPIC, 1, 456, key, body);
        record.headers().add("traceparent", TP.getBytes(StandardCharsets.US_ASCII));
        record.headers().add("baggage", "SENTINEL".getBytes(StandardCharsets.UTF_8));
        var deliveries = mock(DeliveryState.class);
        when(deliveries.load(record)).thenReturn(new DeliveryState.State("PROCESS", 0, 0, null, null), new DeliveryState.State("DLT", 1, 0, null, "INVALID_OR_CONFLICTING"));
        when(deliveries.begin(eq(record), any())).thenReturn(true);
        doAnswer(call -> {
            // BatchSpanProcessor may still be completing an earlier flush started while this span was open.
            await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> assertThat(named("warexis.kafka.dlt")).hasSize(1));
            throw new IllegalStateException("metadata failure");
        }).when(deliveries).complete(record);
        var kafka = (KafkaTemplate<byte[], byte[]>) mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class))).thenAnswer(call -> {
            ProducerRecord<byte[], byte[]> sent = call.getArgument(0);
            assertThat(sent.key()).isSameAs(key); assertThat(sent.value()).isSameAs(body);
            assertThat(sent.partition()).isEqualTo(1); assertThat(sent.topic()).isEqualTo(KafkaConfiguration.DLT);
            assertThat(sent.headers()).hasSize(9);
            assertThat(TraceParent.fromHeaders(sent.headers())).isEqualTo(tracing.currentTraceparent());
            assertThat(sent.headers().lastHeader("baggage")).isNull();
            assertThat(named("warexis.kafka.dlt")).isEmpty();
            return CompletableFuture.completedFuture(null);
        });
        var ack = mock(org.springframework.kafka.support.Acknowledgment.class);
        new InventoryOutcomeListener(outcomes, deliveries, kafka, mock(org.springframework.kafka.config.KafkaListenerEndpointRegistry.class),
                new OperationalMetrics(null, false), tracing).receive(record, ack, mock(org.apache.kafka.clients.consumer.Consumer.class));
        assertThat(named("warexis.kafka.process").getFirst().getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
        var span = named("warexis.kafka.dlt").getFirst();
        assertThat(span.getAttributes().get(AttributeKey.stringKey("warexis.outcome"))).isEqualTo("success");
        assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.UNSET);
        verify(kafka, times(1)).send(any(ProducerRecord.class)); verifyNoInteractions(ack);
    }

    static void assertSafe(SpanData span) {
        assertThat(span.toString()).doesNotContain("SENTINEL", "private", "SELECT", "/home/", "Bearer");
        assertThat(span.getEvents()).isEmpty(); assertThat(span.getStatus().getDescription()).isEmpty();
        assertThat(span.getResource().getAttributes()).isEqualTo(Attributes.builder().put("service.name", "order-service").build());
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
