package io.github.panteliszara.warexis.fulfillment;

import io.github.panteliszara.warexis.fulfillment.application.*;
import io.github.panteliszara.warexis.fulfillment.messaging.*;
import io.github.panteliszara.warexis.fulfillment.observability.*;
import io.micrometer.tracing.Tracer;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import tools.jackson.databind.json.JsonMapper;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "WAREXIS_OAUTH2_ISSUER_URI=https://issuer.example.test/realms/warexis",
        "WAREXIS_OAUTH2_AUDIENCE=warexis-api", "management.tracing.sampling.probability=1.0"})
@Import({FulfillmentServiceTests.PostgresConfiguration.class, TracingIntegrationTests.ExportConfiguration.class})
class TracingIntegrationTests {
    static final String FIRST = "00-11111111111111111111111111111111-2222222222222222-01";
    static final String OTHER = "00-33333333333333333333333333333333-4444444444444444-01";
    static final List<SpanData> EXPORTED = new CopyOnWriteArrayList<>();
    @Autowired FulfillmentService service;
    @Autowired DispatchService dispatch;
    @Autowired JdbcClient jdbc;
    @Autowired JsonMapper json;
    @MockitoSpyBean TraceSupport tracing;
    @Autowired Tracer tracer;
    @Autowired SdkTracerProvider provider;
    @Autowired org.springframework.context.ApplicationContext context;
    @Autowired javax.sql.DataSource dataSource;
    @Autowired org.springframework.transaction.PlatformTransactionManager transactions;
    @LocalServerPort int port;

    @BeforeEach void clearSpans() { flush(); EXPORTED.clear(); }
    void flush() { provider.forceFlush().join(5, java.util.concurrent.TimeUnit.SECONDS); }
    List<SpanData> spans(String name) { flush(); return EXPORTED.stream().filter(s -> s.getName().equals(name)).toList(); }
    String origin(UUID id) { return jdbc.sql("SELECT confirmation_traceparent FROM fulfillments WHERE id=:id")
            .param("id", id).query(String.class).list().getFirst(); }
    String outbox(UUID id) { return jdbc.sql("SELECT traceparent FROM outbox_events WHERE aggregate_id=:id")
            .param("id", id).query(String.class).list().getFirst(); }

    @Test void migrationsKeepNullableBoundedMetadataAndFourColumnProcessedInsert() {
        for (var name : List.of("traceparent", "confirmation_traceparent")) {
            var row = jdbc.sql("SELECT is_nullable,character_maximum_length FROM information_schema.columns "
                    + "WHERE table_schema='public' AND column_name=:name").param("name", name).query().singleRow();
            assertThat(row).containsEntry("is_nullable", "YES").containsEntry("character_maximum_length", 55);
        }
        assertThat(jdbc.sql("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' "
                + "AND table_name='processed_confirmations'").query(Long.class).single()).isEqualTo(4L);
    }

    @Test void firstNullIsImmutableAcrossExactReplayMatchingAliasAndTerminalState() throws Exception {
        var in = FulfillmentServiceTests.input(); service.accept(in); var plan = service.byOrder(in.orderId());
        assertThat(origin(plan.id())).isNull();
        tracing.attempt(TraceSupport.Operation.PROCESS, FIRST, "OrderConfirmed", () -> service.accept(in));
        tracing.attempt(TraceSupport.Operation.PROCESS, OTHER, "OrderConfirmed", () -> service.accept(FulfillmentServiceTests.alias(in)));
        dispatch.ship(plan.id(), "NULL-" + UUID.randomUUID());
        assertThat(origin(plan.id())).isNull();
        assertThat(service.get(plan.id()).confirmationEventId()).isEqualTo(in.eventId());
        assertThat(spans("warexis.fulfillment.dispatch").getLast().getLinks()).isEmpty();
    }

    @Test void sampledAndUnsampledConsumerContextsPersistOnlyFirstInsertAcrossAliasesAndConflicts() throws Exception {
        for (var parent : List.of(FIRST, FIRST.substring(0, 53) + "00")) {
            var in = FulfillmentServiceTests.input(); var captured = new AtomicReference<String>();
            tracing.attempt(TraceSupport.Operation.PROCESS, parent, "OrderConfirmed", () -> {
                captured.set(tracing.currentTraceparent()); return service.accept(in);
            });
            var plan = service.byOrder(in.orderId());
            assertThat(captured.get()).startsWith(parent.substring(0, 36)).endsWith(parent.substring(53));
            assertThat(origin(plan.id())).isEqualTo(captured.get());
            var original = jdbc.sql("SELECT * FROM fulfillments WHERE id=:id").param("id", plan.id()).query().singleRow();
            tracing.attempt(TraceSupport.Operation.PROCESS, OTHER, "OrderConfirmed", () -> service.accept(in));
            tracing.attempt(TraceSupport.Operation.PROCESS, OTHER, "OrderConfirmed", () -> service.accept(FulfillmentServiceTests.alias(in)));
            var conflict = new Confirmation(UUID.randomUUID(), in.eventType(), in.occurredAt(), in.orderId(),
                    UUID.randomUUID(), in.causationId(), in.lines());
            assertThatThrownBy(() -> tracing.attempt(TraceSupport.Operation.PROCESS, OTHER, "OrderConfirmed", () -> service.accept(conflict)))
                    .isInstanceOf(PoisonMessageException.class);
            assertThat(jdbc.sql("SELECT * FROM fulfillments WHERE id=:id").param("id", plan.id()).query().singleRow()).isEqualTo(original);
        }
    }

    @Test void malformedAndOversizeCaptureIsNullBeforeSqlForBothInsertSites() {
        for (var invalid : List.of("x".repeat(100000), "bad\u0000context", FIRST.replace("1111", "AAAA"))) {
            doReturn(invalid).when(tracing).currentTraceparent();
            var in = FulfillmentServiceTests.input(); service.accept(in); var plan = service.byOrder(in.orderId());
            service.ship(plan.id(), "INVALID-" + UUID.randomUUID());
            assertThat(origin(plan.id())).isNull(); assertThat(outbox(plan.id())).isNull();
            var body = jdbc.sql("SELECT event_json::text FROM outbox_events WHERE aggregate_id=:id")
                    .param("id", plan.id()).query(String.class).single();
            assertThat(body).doesNotContain("traceparent");
            EventJson.shipped(in.orderId().toString().getBytes(StandardCharsets.UTF_8), body.getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test void nativeAuthenticatedHttpParentsDispatchAndLinksImmutableConfirmationWithoutPrivateExport() throws Exception {
        var in = FulfillmentServiceTests.input();
        tracing.attempt(TraceSupport.Operation.PROCESS, FIRST, "OrderConfirmed", () -> service.accept(in));
        var plan = service.byOrder(in.orderId()); var first = origin(plan.id());
        var reference = "PRIVATE-" + UUID.randomUUID();
        try (var client = HttpClient.newHttpClient()) {
            var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/api/fulfillments/" + plan.id() + "/ship?secret=PRIVATE_QUERY"))
                    .timeout(Duration.ofSeconds(10)).header("Content-Type", "application/json")
                    .header("Authorization", "Bearer " + TestJwtConfiguration.operator())
                    .header("Traceparent", OTHER.substring(0, 53) + "03")
                    .header("baggage", "private=PRIVATE_BAGGAGE").header("tracestate", "private=PRIVATE_STATE")
                    .POST(HttpRequest.BodyPublishers.ofString("{\"shipmentReference\":\"" + reference + "\"}")).build();
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body()).doesNotContain("traceparent", "confirmationTraceparent");
        }
        var http = spans("http.request").getLast(); var shipped = spans("warexis.fulfillment.dispatch").getLast();
        assertThat(http.getTraceId()).isEqualTo(OTHER.substring(3, 35));
        assertThat(http.getParentSpanId()).isEqualTo(OTHER.substring(36, 52));
        assertThat(shipped.getTraceId()).isEqualTo(http.getTraceId());
        // Spring Security can own legitimate intermediate scopes within the native HTTP request.
        var graph = new HashMap<String, SpanData>();
        EXPORTED.forEach(s -> graph.put(s.getSpanId(), s));
        String ancestor = shipped.getParentSpanId();
        var visited = new HashSet<String>();
        while (!ancestor.equals(http.getSpanId())) {
            assertThat(visited.add(ancestor)).isTrue();
            assertThat(graph).containsKey(ancestor);
            var intermediate = graph.get(ancestor);
            assertThat(intermediate.getTraceId()).isEqualTo(http.getTraceId());
            ancestor = intermediate.getParentSpanId();
        }
        assertThat(shipped.getLinks()).hasSize(1);
        assertThat(shipped.getLinks().getFirst().getSpanContext().getTraceId()).isEqualTo(first.substring(3, 35));
        assertThat(shipped.getLinks().getFirst().getSpanContext().getSpanId()).isEqualTo(first.substring(36, 52));
        assertThat(outbox(plan.id())).isEqualTo("00-" + shipped.getTraceId() + "-" + shipped.getSpanId() + "-01");
        assertThat(origin(plan.id())).isEqualTo(first);
        for (var span : EXPORTED) {
            assertThat(span.getResource().getAttributes().asMap()).hasSize(1);
            assertThat(span.getResource().getAttribute(AttributeKey.stringKey("service.name"))).isEqualTo("fulfillment-service");
            assertThat(span.getEvents()).isEmpty(); assertThat(span.getStatus().getDescription()).isEmpty();
            assertThat(span.getStatus().getStatusCode()).isNotEqualTo(StatusCode.OK);
            assertThat(span.getAttributes().toString()).doesNotContain("PRIVATE", reference, plan.id().toString(), "Bearer", "jwt", "url", "route", "path");
        }
        assertThat(tracer.currentSpan()).isNull();
    }

    @Test void actualDeferredCommitFailureRollsBackShipmentAndOutboxAndCannotMarkDispatchSuccess() throws Exception {
        var in = FulfillmentServiceTests.input();
        tracing.attempt(TraceSupport.Operation.PROCESS, FIRST, "OrderConfirmed", () -> service.accept(in));
        var plan = service.byOrder(in.orderId()); var first = origin(plan.id());
        jdbc.sql("CREATE TABLE trace_commit_guard(value VARCHAR(55) PRIMARY KEY)").update();
        jdbc.sql("ALTER TABLE outbox_events ADD CONSTRAINT trace_commit_fk FOREIGN KEY(traceparent) "
                + "REFERENCES trace_commit_guard(value) DEFERRABLE INITIALLY DEFERRED NOT VALID").update();
        var http = tracer.nextSpan().name("simulated.http").start();
        try (var current = tracer.withSpan(http)) {
            assertThatThrownBy(() -> dispatch.ship(plan.id(), "COMMIT-" + UUID.randomUUID()))
                    .isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
            assertThat(tracer.currentSpan().context().spanId()).isEqualTo(http.context().spanId());
        } finally {
            http.end(); jdbc.sql("ALTER TABLE outbox_events DROP CONSTRAINT trace_commit_fk").update();
            jdbc.sql("DROP TABLE trace_commit_guard").update();
        }
        assertThat(service.get(plan.id())).isEqualTo(plan);
        assertThat(jdbc.sql("SELECT COUNT(*) FROM outbox_events WHERE aggregate_id=:id").param("id", plan.id()).query(Long.class).single()).isZero();
        assertThat(origin(plan.id())).isEqualTo(first);
        var failed = spans("warexis.fulfillment.dispatch").getLast();
        assertThat(failed.getAttributes().get(AttributeKey.stringKey("warexis.outcome"))).isEqualTo("failure");
        assertThat(failed.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
    }

    @Test void optionalOriginLookupRuntimeFailureDoesNotRetryOrPreventSoleShipInvocation() {
        var app = mock(FulfillmentService.class); var id = UUID.randomUUID(); var original = new IllegalStateException("private ship");
        when(app.confirmationTraceparent(id)).thenThrow(new IllegalStateException("private lookup"));
        when(app.ship(id, "ONCE")).thenThrow(original);
        var wrapped = new DispatchService(app, tracing);
        assertThat(catchThrowable(() -> wrapped.ship(id, "ONCE"))).isSameAs(original);
        verify(app, times(1)).confirmationTraceparent(id); verify(app, times(1)).ship(id, "ONCE");
        assertThat(spans("warexis.fulfillment.dispatch").getLast().getLinks()).isEmpty();
    }

    @Test @SuppressWarnings("unchecked")
    void processingEndsAfterAcceptCommitButBeforeSeparateCompletionAndAcknowledgment() {
        var in = FulfillmentServiceTests.input(); var record = record(in);
        record.headers().add("traceparent", FIRST.getBytes(StandardCharsets.US_ASCII));
        var state = spy(new DeliveryState(jdbc, properties()));
        doThrow(new IllegalStateException("completion metadata unavailable")).when(state).complete(record);
        var ack = mock(org.springframework.kafka.support.Acknowledgment.class);
        var kafka = (org.springframework.kafka.core.KafkaTemplate<byte[], byte[]>) mock(org.springframework.kafka.core.KafkaTemplate.class);
        var registry = mock(org.springframework.kafka.config.KafkaListenerEndpointRegistry.class);
        var consumer = (org.apache.kafka.clients.consumer.Consumer<byte[], byte[]>) mock(org.apache.kafka.clients.consumer.Consumer.class);
        new ConfirmationListener(service, state, kafka, registry, new OperationalMetrics(null, false), tracing).receive(record, ack, consumer);
        var plan = service.byOrder(in.orderId());
        assertThat(origin(plan.id())).startsWith(FIRST.substring(0, 36));
        var processed = spans("warexis.kafka.process"); assertThat(processed).hasSize(1);
        assertThat(processed.getFirst().getAttributes().get(AttributeKey.stringKey("warexis.outcome"))).isEqualTo("success");
        assertThat(state.load(record).phase()).isEqualTo("PROCESS");
        verify(ack, never()).acknowledge();
        verify(consumer).seek(new org.apache.kafka.common.TopicPartition(record.topic(), record.partition()), record.offset());
        assertThat(tracer.currentSpan()).isNull();
    }

    @Test @SuppressWarnings("unchecked")
    void duplicateAndPausedDeliveryDoNotCreateProcessingAttempts() {
        var in = FulfillmentServiceTests.input(); var record = record(in); var state = new DeliveryState(jdbc, properties());
        state.load(record); state.complete(record);
        var kafka = (org.springframework.kafka.core.KafkaTemplate<byte[], byte[]>) mock(org.springframework.kafka.core.KafkaTemplate.class);
        var ack = mock(org.springframework.kafka.support.Acknowledgment.class);
        var consumer = (org.apache.kafka.clients.consumer.Consumer<byte[], byte[]>) mock(org.apache.kafka.clients.consumer.Consumer.class);
        var listener = new ConfirmationListener(service, state, kafka, mock(org.springframework.kafka.config.KafkaListenerEndpointRegistry.class), new OperationalMetrics(null, false), tracing);
        listener.receive(record, ack, consumer); verify(ack).acknowledge();
        var paused = record(FulfillmentServiceTests.input()); state.load(paused); state.pause(paused);
        listener.receive(paused, mock(org.springframework.kafka.support.Acknowledgment.class), consumer);
        assertThat(spans("warexis.kafka.process")).isEmpty(); assertThat(spans("warexis.kafka.dlt")).isEmpty();
        verifyNoInteractions(kafka);
    }

    @Test @SuppressWarnings("unchecked")
    void dltBrokerAcknowledgmentCarriesOnlyEightDiagnosticsAndOneGeneratedValidatedContext() {
        var in = FulfillmentServiceTests.input(); var record = record(in);
        record.headers().add("traceparent", FIRST.getBytes(StandardCharsets.US_ASCII));
        record.headers().add("baggage", "private=PRIVATE".getBytes(StandardCharsets.UTF_8));
        var state = new DeliveryState(jdbc, properties()); state.load(record); state.toDlt(record, "INVALID_OR_CONFLICTING");
        var kafka = (org.springframework.kafka.core.KafkaTemplate<byte[], byte[]>) mock(org.springframework.kafka.core.KafkaTemplate.class);
        var sent = new AtomicReference<org.apache.kafka.clients.producer.ProducerRecord<byte[], byte[]>>();
        when(kafka.send(any(org.apache.kafka.clients.producer.ProducerRecord.class))).thenAnswer(call -> {
            sent.set(call.getArgument(0)); return java.util.concurrent.CompletableFuture.completedFuture(null);
        });
        var ack = mock(org.springframework.kafka.support.Acknowledgment.class);
        new ConfirmationListener(service, state, kafka, mock(org.springframework.kafka.config.KafkaListenerEndpointRegistry.class),
                new OperationalMetrics(null, false), tracing).receive(record, ack, mock(org.apache.kafka.clients.consumer.Consumer.class));
        verify(ack).acknowledge(); verify(kafka, times(1)).send(any(org.apache.kafka.clients.producer.ProducerRecord.class));
        assertThat(sent.get().key()).isEqualTo(record.key()); assertThat(sent.get().value()).isEqualTo(record.value());
        assertThat(sent.get().headers().toArray()).hasSize(9);
        assertThat(sent.get().headers().lastHeader("baggage")).isNull();
        var dlt = spans("warexis.kafka.dlt").getFirst();
        assertThat(dlt.getParentSpanId()).isEqualTo(FIRST.substring(36, 52));
        assertThat(TraceParent.fromHeaders(sent.get().headers())).isEqualTo("00-" + dlt.getTraceId() + "-" + dlt.getSpanId() + "-01");
        assertThat(dlt.getAttributes().get(AttributeKey.stringKey("warexis.outcome"))).isEqualTo("success");
        assertThat(state.load(record).phase()).isEqualTo("COMPLETE");
    }

    @Test @SuppressWarnings("unchecked")
    void relayAttemptIncludesActualSendAndDurableCheckpointAndDoesNotMutateBusinessBytes() throws Exception {
        var in = FulfillmentServiceTests.input(); service.accept(in); var plan = service.byOrder(in.orderId());
        tracing.attempt(TraceSupport.Operation.PROCESS, FIRST, "OrderConfirmed", () -> service.ship(plan.id(), "RELAY-" + UUID.randomUUID()));
        var persisted = outbox(plan.id()); var body = jdbc.sql("SELECT event_json::text FROM outbox_events WHERE aggregate_id=:id")
                .param("id", plan.id()).query(String.class).single().getBytes(StandardCharsets.UTF_8);
        // Other tests may retain unpublished business rows. This owned run selects only its row.
        jdbc.sql("UPDATE outbox_events SET published_at=GREATEST(CURRENT_TIMESTAMP,occurred_at) WHERE aggregate_id<>:id AND published_at IS NULL")
                .param("id", plan.id()).update();
        var kafka = (org.springframework.kafka.core.KafkaTemplate<byte[], byte[]>) mock(org.springframework.kafka.core.KafkaTemplate.class);
        var sent = new AtomicReference<org.apache.kafka.clients.producer.ProducerRecord<byte[], byte[]>>();
        when(kafka.send(any(org.apache.kafka.clients.producer.ProducerRecord.class))).thenAnswer(call -> {
            sent.set(call.getArgument(0)); return java.util.concurrent.CompletableFuture.completedFuture(null);
        });
        var relay = new OutboxRelay(dataSource, jdbc, kafka, properties(), transactions, new OperationalMetrics(null, false), tracing);
        jdbc.sql("ALTER TABLE outbox_events ADD CONSTRAINT trace_relay_guard CHECK(published_at IS NULL) NOT VALID").update();
        try { relay.publishNext(); } finally { jdbc.sql("ALTER TABLE outbox_events DROP CONSTRAINT trace_relay_guard").update(); }
        var failed = spans("warexis.outbox.publish").getFirst();
        assertThat(failed.getAttributes().get(AttributeKey.stringKey("warexis.outcome"))).isEqualTo("failure");
        assertThat(failed.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
        assertThat(jdbc.sql("SELECT published_at IS NULL FROM outbox_events WHERE aggregate_id=:id").param("id", plan.id()).query(Boolean.class).single()).isTrue();
        jdbc.sql("UPDATE outbox_events SET next_attempt_at=NULL WHERE aggregate_id=:id").param("id", plan.id()).update();
        relay.publishNext();
        var attempts = spans("warexis.outbox.publish"); assertThat(attempts).hasSize(2);
        var success = attempts.getLast();
        assertThat(success.getAttributes().get(AttributeKey.stringKey("warexis.outcome"))).isEqualTo("success");
        assertThat(success.getParentSpanId()).isEqualTo(persisted.substring(36, 52));
        assertThat(sent.get().key()).isEqualTo(in.orderId().toString().getBytes(StandardCharsets.UTF_8));
        assertThat(sent.get().value()).isEqualTo(body);
        assertThat(TraceParent.fromHeaders(sent.get().headers())).isEqualTo("00-" + success.getTraceId() + "-" + success.getSpanId() + "-01");
        assertThat(outbox(plan.id())).isEqualTo(persisted);
        verify(kafka, times(2)).send(any(org.apache.kafka.clients.producer.ProducerRecord.class));
    }

    @Test void missingMalformedAndDuplicateKafkaContextStartExplicitRootsAndRestoreAmbient() throws Exception {
        var outer = tracer.nextSpan().name("ambient.scheduler").start();
        try (var scope = tracer.withSpan(outer)) {
            for (int n = 0; n < 3; n++) {
                var headers = new org.apache.kafka.common.header.internals.RecordHeaders();
                if (n == 1) headers.add("traceparent", "malformed".getBytes(StandardCharsets.US_ASCII));
                if (n == 2) { headers.add("traceparent", FIRST.getBytes(StandardCharsets.US_ASCII)); headers.add("traceparent", FIRST.getBytes(StandardCharsets.US_ASCII)); }
                tracing.attempt(TraceSupport.Operation.PROCESS, TraceParent.fromHeaders(headers), "unknown", () -> {
                    assertThat(tracing.currentTraceparent()).doesNotStartWith("00-" + outer.context().traceId()); return null;
                });
                assertThat(tracing.currentTraceparent()).contains(outer.context().spanId());
            }
        } finally { outer.end(); }
        assertThat(spans("warexis.kafka.process")).hasSize(3)
                .allSatisfy(span -> assertThat(span.getParentSpanId()).isEqualTo("0000000000000000"));
        assertThat(tracer.currentSpan()).isNull();
    }

    @Test void nativeHttpRejectsDuplicateAndMalformedParentsWithoutBaggageOrCompetingExporters() throws Exception {
        try (var client = HttpClient.newHttpClient()) {
            for (var invalid : List.of("malformed", "duplicate")) {
                var builder = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/actuator/health"));
                if (invalid.equals("duplicate")) builder.header("Traceparent", FIRST).header("traceparent", FIRST);
                else builder.header("traceparent", invalid);
                assertThat(client.send(builder.GET().build(), HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(200);
                var http = spans("http.request").getLast();
                assertThat(http.getParentSpanId()).isEqualTo("0000000000000000");
            }
        }
        assertThat(context.getBeansOfType(SpanExporter.class).values()).allSatisfy(exporter -> assertThat(exporter).isInstanceOf(PrivacySpanExporter.class));
        assertThat(context.getBeanProvider(io.micrometer.registry.otlp.OtlpMeterRegistry.class).stream()).isEmpty();
        assertThat(context.getBeansOfType(io.opentelemetry.sdk.logs.export.LogRecordExporter.class)).isEmpty();
        assertThat(io.opentelemetry.api.baggage.Baggage.current().asMap()).isEmpty();
    }

    @Test void actualV1ToV2MigrationPreservesLegacyNullOriginsAndOriginalBusinessRows() {
        var schema = "legacy_trace";
        var migration = org.flywaydb.core.Flyway.configure().dataSource(dataSource).schemas(schema).defaultSchema(schema)
                .target("1").load();
        migration.migrate();
        var id = UUID.randomUUID(); var order = UUID.randomUUID(); var reservation = UUID.randomUUID(); var event = UUID.randomUUID();
        jdbc.sql("INSERT INTO legacy_trace.fulfillments(id,order_id,reservation_id,confirmation_event_id,confirmation_causation_id,status,created_at,updated_at) "
                + "VALUES(:id,:order,:reservation,:event,:cause,'PENDING',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)")
                .param("id", id).param("order", order).param("reservation", reservation).param("event", event).param("cause", UUID.randomUUID()).update();
        jdbc.sql("INSERT INTO legacy_trace.processed_confirmations VALUES(:event,:id,'{}',CURRENT_TIMESTAMP)")
                .param("event", event).param("id", id).update();
        var original = jdbc.sql("SELECT * FROM legacy_trace.fulfillments WHERE id=:id").param("id", id).query().singleRow();
        org.flywaydb.core.Flyway.configure().dataSource(dataSource).schemas(schema).defaultSchema(schema).load().migrate();
        var after = new HashMap<>(jdbc.sql("SELECT * FROM legacy_trace.fulfillments WHERE id=:id").param("id", id).query().singleRow());
        assertThat(after.remove("confirmation_traceparent")).isNull(); assertThat(after).isEqualTo(original);
        jdbc.sql("INSERT INTO legacy_trace.processed_confirmations VALUES(:event,:id,'{}',CURRENT_TIMESTAMP)")
                .param("event", UUID.randomUUID()).param("id", id).update();
        assertThat(jdbc.sql("SELECT COUNT(*) FROM legacy_trace.processed_confirmations").query(Long.class).single()).isEqualTo(2L);
    }

    @Test void failedSetupSuppressesAmbientCaptureOnceAndRestoresNativeScopeForAttemptAndDispatch() throws Exception {
        var outer = tracer.nextSpan().name("ambient.http").start();
        var broken = spy(tracer);
        doThrow(new IllegalStateException("private telemetry setup")).when(broken).spanBuilder();
        var support = new TraceSupport(broken, context.getBean(io.micrometer.tracing.propagation.Propagator.class));
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        try (var scope = tracer.withSpan(outer)) {
            String original = support.currentTraceparent();
            assertThat(original).contains(outer.context().spanId());
            support.attempt(TraceSupport.Operation.PROCESS, "invalid", "unknown", () -> {
                calls.incrementAndGet(); assertThat(support.currentTraceparent()).isNull(); return null;
            });
            assertThat(calls).hasValue(1); assertThat(support.currentTraceparent()).isEqualTo(original);
            var originalFailure = new IllegalArgumentException("private dispatch failure");
            assertThat(catchThrowable(() -> support.dispatch(FIRST, () -> {
                calls.incrementAndGet(); assertThat(support.currentTraceparent()).isNull(); throw originalFailure;
            }))).isSameAs(originalFailure);
            assertThat(calls).hasValue(2); assertThat(support.currentTraceparent()).isEqualTo(original);
            assertThat(tracer.currentSpan().context().spanId()).isEqualTo(outer.context().spanId());
        } finally { outer.end(); }
        assertThat(support.currentTraceparent()).isNull();
    }

    MessagingProperties properties() { return new MessagingProperties(true, Duration.ofMillis(1), Duration.ofMillis(50)); }
    org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> record(Confirmation in) {
        return new org.apache.kafka.clients.consumer.ConsumerRecord<>(KafkaConfiguration.ORDER_TOPIC, 0,
                Math.abs(UUID.randomUUID().getMostSignificantBits()), in.orderId().toString().getBytes(StandardCharsets.UTF_8),
                EventJsonTests.envelope(in).getBytes(StandardCharsets.UTF_8));
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class ExportConfiguration {
        @Bean SpanExporter recordingTraceExporter() {
            return new SpanExporter() {
                public CompletableResultCode export(Collection<SpanData> spans) { EXPORTED.addAll(spans); return CompletableResultCode.ofSuccess(); }
                public CompletableResultCode flush() { return CompletableResultCode.ofSuccess(); }
                public CompletableResultCode shutdown() { return CompletableResultCode.ofSuccess(); }
            };
        }
    }
}
