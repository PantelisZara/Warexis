package io.github.panteliszara.warexis.inventory.observability;

import io.github.panteliszara.warexis.inventory.messaging.*;
import io.github.panteliszara.warexis.inventory.reservations.application.*;
import io.github.panteliszara.warexis.inventory.reservations.domain.ReservationLine;
import io.github.panteliszara.warexis.inventory.stock.application.StockItemService;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelPropagator;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/** Actual service/proxy/schema and PostgreSQL boundaries; transport completion is controlled here. */
@SpringBootTest(properties = {"WAREXIS_OAUTH2_ISSUER_URI=https://issuer.test/realms/warexis",
        "WAREXIS_OAUTH2_AUDIENCE=warexis-api"})
@org.springframework.context.annotation.Import(io.github.panteliszara.warexis.inventory.security.TestJwtConfiguration.class)
@Testcontainers
class InventoryTracingPostgresTests {
    private static final String PARENT = "00-12345678901234567890123456789012-1234567890123456-01";
    private static final String OTHER = "00-abcdefabcdefabcdefabcdefabcdefab-abcdefabcdefabcd-01";
    @Container @ServiceConnection static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18.6");
    @Autowired InventoryReservationService reservations;
    @Autowired StockItemService stocks;
    @Autowired JdbcTemplate jdbc;
    @Autowired DataSource dataSource;
    @Autowired JsonMapper mapper;
    @Autowired WorkflowEventValidator validator;
    @MockitoSpyBean TraceSupport appTracing;
    final List<SpanData> spans = new ArrayList<>();
    SdkTracerProvider provider;
    Tracer tracer;
    TraceSupport tracing;
    MessagingProperties properties;
    ConsumerDeliveryStore delivery;
    @SuppressWarnings("unchecked") KafkaTemplate<byte[], byte[]> transport = mock(KafkaTemplate.class);
    final AtomicInteger offsets = new AtomicInteger();

    @BeforeEach void setup() {
        jdbc.execute("TRUNCATE outbox_events, processed_order_events, inventory_reservations, stock_items, consumer_delivery_state");
        jdbc.update("UPDATE outbox_relay_control SET paused = FALSE, failure_code = NULL");
        var capture = new SpanExporter() {
            public CompletableResultCode export(Collection<SpanData> data) { spans.addAll(data); return CompletableResultCode.ofSuccess(); }
            public CompletableResultCode flush() { return CompletableResultCode.ofSuccess(); }
            public CompletableResultCode shutdown() { return CompletableResultCode.ofSuccess(); }
        };
        provider = SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(new PrivacySpanExporter(capture))).build();
        var sdk = provider.get("inventory-test");
        tracer = new OtelTracer(sdk, new OtelCurrentTraceContext(), event -> {});
        tracing = new TraceSupport(tracer, new OtelPropagator(
                ContextPropagators.create(new TracingConfiguration.StrictTraceparent()), sdk));
        properties = new MessagingProperties(true, Duration.ofMillis(1), Duration.ofHours(24));
        delivery = new ConsumerDeliveryStore(jdbc, properties);
        when(transport.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
    }
    @AfterEach void close() { provider.close(); assertThat(tracer.currentSpan()).isNull(); }

    @Test void processPersistsItsOwnContextAndPublisherIncludesDurableMetadataBeforeSpanEnd() throws Exception {
        var input = input();
        var record = record(input, PARENT);
        var ack = mock(Acknowledgment.class);
        doAnswer(call -> {
            assertThat(attempts("warexis.kafka.process")).hasSize(1);
            assertThat(delivery.load(record).phase()).isEqualTo("COMPLETE");
            return null;
        }).when(ack).acknowledge();
        listener().receive(record, ack, consumer());
        var process = attempts("warexis.kafka.process").getFirst();
        assertThat(process.getParentSpanId()).isEqualTo(PARENT.substring(36, 52));
        assertSuccess(process);
        var durable = row(input.orderId());
        assertThat(durable.get("traceparent")).isEqualTo(context(process));
        assertThat(durable.get("event_json").toString()).doesNotContain("traceparent", "tracestate", "baggage");
        var sent = new ArrayList<ProducerRecord<byte[], byte[]>>();
        when(transport.send(any(ProducerRecord.class))).thenAnswer(call -> {
            sent.add(call.getArgument(0));
            assertThat(attempts("warexis.outbox.publish")).isEmpty();
            assertThat(row(input.orderId()).get("published_at")).isNull();
            return CompletableFuture.completedFuture(null);
        });
        publisher().publishNext();
        var publish = attempts("warexis.outbox.publish").getFirst();
        assertSuccess(publish);
        assertThat(publish.getParentSpanId()).isEqualTo(process.getSpanId());
        assertThat(TraceParent.fromHeaders(sent.getFirst().headers())).isEqualTo(context(publish));
        assertThat(sent.getFirst().key()).containsExactly(input.orderId().toString().getBytes(StandardCharsets.UTF_8));
        assertThat(sent.getFirst().value()).containsExactly(durable.get("event_json").toString().getBytes(StandardCharsets.UTF_8));
        assertThat(row(input.orderId()).get("published_at")).isNotNull();
        assertThat(row(input.orderId()).get("event_id")).isEqualTo(durable.get("event_id"));
        verify(ack).acknowledge();
    }

    @Test void deferredCommitFailureRollsBackBusinessAndMarksEveryAttemptFailed() {
        var input = input();
        var record = record(input, PARENT);
        var ack = mock(Acknowledgment.class);
        jdbc.execute("CREATE FUNCTION tracing_commit_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'private SQL failure'; END $$");
        jdbc.execute("CREATE CONSTRAINT TRIGGER tracing_commit_failure AFTER INSERT ON outbox_events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION tracing_commit_failure()");
        try { listener().receive(record, ack, consumer()); }
        finally {
            jdbc.execute("DROP TRIGGER tracing_commit_failure ON outbox_events");
            jdbc.execute("DROP FUNCTION tracing_commit_failure()");
        }
        assertThat(attempts("warexis.kafka.process")).hasSize(4).allSatisfy(this::assertFailure);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM inventory_reservations", Integer.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_events", Integer.class)).isZero();
        assertThat(stocks.get(input.lines().getFirst().sku()).getReserved()).isZero();
        assertThat(delivery.load(record).processingAttempts()).isEqualTo(4);
        assertThat(delivery.load(record).phase()).isEqualTo("COMPLETE");
        assertSuccess(attempts("warexis.kafka.dlt").getFirst());
        assertThat(spans.toString()).doesNotContain("private SQL failure");
        verify(ack).acknowledge();
    }

    @Test void successfulProcessIsSeparateFromFailedCompletionAndAck() {
        var input = input();
        var record = record(input, PARENT);
        var ack = mock(Acknowledgment.class);
        doThrow(new IllegalStateException("private ACK failure")).when(ack).acknowledge();
        listener().receive(record, ack, consumer());
        assertSuccess(attempts("warexis.kafka.process").getFirst());
        assertThat(delivery.load(record).phase()).isEqualTo("COMPLETE");
        assertThat(row(input.orderId()).get("traceparent")).isEqualTo(context(attempts("warexis.kafka.process").getFirst()));
        assertThat(stocks.get(input.lines().getFirst().sku()).getReserved()).isEqualTo(1);
        listener().receive(record, mock(Acknowledgment.class), consumer());
        assertThat(attempts("warexis.kafka.process")).hasSize(1);
    }

    @Test void publisherMetadataFailureIsFailedDespiteBrokerAcknowledgment() {
        var input = input();
        reservations.process(input);
        jdbc.execute("ALTER TABLE outbox_events ADD CONSTRAINT tracing_no_publish CHECK (published_at IS NULL) NOT VALID");
        try { publisher().publishNext(); }
        finally { jdbc.execute("ALTER TABLE outbox_events DROP CONSTRAINT tracing_no_publish"); }
        assertFailure(attempts("warexis.outbox.publish").getFirst());
        verify(transport).send(any(ProducerRecord.class));
        assertThat(row(input.orderId()).get("published_at")).isNull();
        assertThat(row(input.orderId()).get("publication_attempts")).isEqualTo(1);
    }

    @Test void brokerFailureDoesNotPublishOrInventSuccess() {
        var input = input();
        reservations.process(input);
        when(transport.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("private broker failure")));
        publisher().publishNext();
        assertFailure(attempts("warexis.outbox.publish").getFirst());
        assertThat(row(input.orderId()).get("published_at")).isNull();
        assertThat(row(input.orderId()).get("publication_attempts")).isEqualTo(1);
    }

    @Test void originalContextAndBusinessBytesSurviveReplayAliasAndConflict() throws Exception {
        var input = input();
        tracing.attempt(TraceSupport.Operation.PROCESS, PARENT, "OrderPlaced", () -> reservations.process(input));
        var before = row(input.orderId());
        tracing.attempt(TraceSupport.Operation.PROCESS, OTHER, "OrderPlaced", () -> reservations.process(input));
        var alias = new OrderPlaced(UUID.randomUUID(), input.orderId(), input.lines());
        tracing.attempt(TraceSupport.Operation.PROCESS, OTHER, "OrderPlaced", () -> reservations.process(alias));
        var changed = new OrderPlaced(UUID.randomUUID(), input.orderId(), List.of(new ReservationLine(input.lines().getFirst().orderLineId(), input.lines().getFirst().sku(), 2)));
        assertThatThrownBy(() -> tracing.attempt(TraceSupport.Operation.PROCESS, OTHER, "OrderPlaced", () -> reservations.process(changed)))
                .isInstanceOf(ConflictingOrderPlacedException.class);
        assertThat(row(input.orderId())).isEqualTo(before);
        assertThat(stocks.get(input.lines().getFirst().sku()).getReserved()).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM processed_order_events", Integer.class)).isEqualTo(2);
    }

    @Test void originalNullIsNotReplacedByLaterAliasContext() throws Exception {
        var input = input();
        reservations.process(input);
        var before = row(input.orderId());
        assertThat(before.get("traceparent")).isNull();
        tracing.attempt(TraceSupport.Operation.PROCESS, PARENT, "OrderPlaced", () -> reservations.process(new OrderPlaced(UUID.randomUUID(), input.orderId(), input.lines())));
        assertThat(row(input.orderId())).isEqualTo(before);
    }

    @ParameterizedTest @ValueSource(strings = {"invalid", "00-12345678901234567890123456789012-1234567890123456-ff", "oversized"})
    void outboxNormalizesBeforeSql(String candidate) {
        var input = input();
        var raw = candidate.equals("oversized") ? "x".repeat(10000) : candidate;
        doReturn(raw).when(appTracing).currentTraceparent();
        reservations.process(input);
        assertThat(row(input.orderId()).get("traceparent")).isEqualTo(TraceParent.normalize(raw));
    }

    @ParameterizedTest @NullAndEmptySource @ValueSource(strings = {"invalid", "01-12345678901234567890123456789012-1234567890123456-01"})
    void absentOrInvalidDurableContextStartsRootDespiteAmbient(String parent) {
        var input = input();
        reservations.process(input);
        jdbc.update("UPDATE outbox_events SET traceparent = ? WHERE correlation_id = ?", parent, input.orderId());
        var outer = tracer.nextSpan().name("ambient").start();
        try (var scope = tracer.withSpan(outer)) {
            publisher().publishNext();
            assertThat(tracer.currentSpan().context().spanId()).isEqualTo(outer.context().spanId());
        } finally { outer.end(); }
        var publish = attempts("warexis.outbox.publish").getFirst();
        assertThat(publish.getParentSpanContext().isValid()).isFalse();
        assertThat(publish.getTraceId()).isNotEqualTo(outer.context().traceId());
    }

    @ParameterizedTest @ValueSource(strings = {"missing", "invalid", "duplicate"})
    void invalidRemoteContextStartsRootAndKeepsBusiness(String mode) {
        var input = input();
        var record = record(input, mode.equals("missing") ? null : mode.equals("invalid") ? "invalid" : PARENT);
        if (mode.equals("duplicate")) record.headers().add("traceparent", PARENT.getBytes(StandardCharsets.US_ASCII));
        var outer = tracer.nextSpan().name("ambient").start();
        try (var scope = tracer.withSpan(outer)) {
            listener().receive(record, mock(Acknowledgment.class), consumer());
            assertThat(tracer.currentSpan().context().spanId()).isEqualTo(outer.context().spanId());
        } finally { outer.end(); }
        var process = attempts("warexis.kafka.process").getFirst();
        assertThat(process.getParentSpanContext().isValid()).isFalse();
        assertThat(process.getTraceId()).isNotEqualTo(outer.context().traceId());
        assertSuccess(process);
    }

    @Test void unsampledContextPersistsAndPropagatesWithoutExportedAttempts() {
        var input = input();
        var parent = PARENT.substring(0, 53) + "00";
        listener().receive(record(input, parent), mock(Acknowledgment.class), consumer());
        var durable = (String) row(input.orderId()).get("traceparent");
        assertThat(durable).startsWith(parent.substring(0, 36)).endsWith("-00");
        var sent = new ArrayList<ProducerRecord<byte[], byte[]>>();
        when(transport.send(any(ProducerRecord.class))).thenAnswer(call -> { sent.add(call.getArgument(0)); return CompletableFuture.completedFuture(null); });
        publisher().publishNext();
        assertThat(TraceParent.fromHeaders(sent.getFirst().headers())).startsWith(parent.substring(0, 36)).endsWith("-00");
        assertThat(spans).isEmpty();
        assertThat(row(input.orderId()).get("published_at")).isNotNull();
    }

    @Test void dltSpanMeansBrokerAckAndRetainsEightDiagnosticsPlusOnlySafeContext() {
        var input = input();
        var poison = new ConsumerRecord<byte[], byte[]>("warexis.order.events.v1", 0, offsets.incrementAndGet(), input.orderId().toString().getBytes(StandardCharsets.UTF_8), new byte[]{(byte) 0xc3, 0x28});
        poison.headers().add("traceparent", PARENT.getBytes(StandardCharsets.US_ASCII));
        poison.headers().add("baggage", "private-value".getBytes(StandardCharsets.UTF_8));
        var sent = new ArrayList<ProducerRecord<byte[], byte[]>>();
        when(transport.send(any(ProducerRecord.class))).thenAnswer(call -> { sent.add(call.getArgument(0)); return CompletableFuture.completedFuture(null); });
        listener().receive(poison, mock(Acknowledgment.class), consumer());
        assertThat(attempts("warexis.kafka.process")).isEmpty();
        var dlt = attempts("warexis.kafka.dlt").getFirst();
        assertSuccess(dlt);
        assertThat(dlt.getParentSpanId()).isEqualTo(PARENT.substring(36, 52));
        assertThat(sent.getFirst().headers()).hasSize(9);
        var keys = new ArrayList<String>();
        sent.getFirst().headers().forEach(header -> keys.add(header.key()));
        assertThat(keys).containsExactlyInAnyOrder("warexis-source-topic", "warexis-source-partition",
                "warexis-source-offset", "warexis-consumer-group", "warexis-failure-code",
                "warexis-processing-attempts", "warexis-dlt-attempts", "warexis-failed-at", "traceparent");
        assertThat(sent.getFirst().headers().lastHeader("baggage")).isNull();
        assertThat(TraceParent.fromHeaders(sent.getFirst().headers())).isEqualTo(context(dlt));
        assertThat(sent.getFirst().value()).containsExactly(poison.value());
        assertThat(sent.getFirst().key()).containsExactly(poison.key());
    }

    @Test void durableContextReloadsInSeparateJavaProcessWithSameBusinessIdsAndBytes() throws Exception {
        var input = input();
        tracing.attempt(TraceSupport.Operation.PROCESS, PARENT, "OrderPlaced", () -> reservations.process(input));
        var before = row(input.orderId());
        var child = new ProcessBuilder(System.getProperty("java.home") + "/bin/java", "-cp", System.getProperty("java.class.path"), InventoryTraceRestartReader.class.getName()).start();
        child.getOutputStream().write(mapper.writeValueAsBytes(Map.of("url", postgres.getJdbcUrl(), "user", postgres.getUsername(), "password", postgres.getPassword(), "orderId", input.orderId().toString())));
        child.getOutputStream().close();
        assertThat(child.waitFor(20, TimeUnit.SECONDS)).isTrue();
        assertThat(child.exitValue()).withFailMessage(new String(child.getErrorStream().readAllBytes(), StandardCharsets.UTF_8)).isZero();
        var result = mapper.readTree(child.getInputStream().readAllBytes());
        assertThat(result.path("pid").asLong()).isNotEqualTo(ProcessHandle.current().pid());
        assertThat(result.path("traceparent").asString()).isEqualTo(before.get("traceparent"));
        assertThat(result.path("eventId").asString()).isEqualTo(before.get("event_id").toString());
        assertThat(result.path("payload").asString()).isEqualTo(before.get("event_json"));
        assertThat(result.path("key").asString()).isEqualTo(input.orderId().toString());
        assertThat(row(input.orderId())).isEqualTo(before);
    }

    @Test void v4UpgradePreservesExistingV3RowsAndLeavesContextNullable() {
        var input = input();
        reservations.process(input);
        var before = row(input.orderId());
        var baseline = org.flywaydb.core.Flyway.configure().dataSource(dataSource)
                .schemas("tracing_upgrade").defaultSchema("tracing_upgrade").target("3").load();
        baseline.migrate();
        try {
            jdbc.update("INSERT INTO tracing_upgrade.inventory_reservations SELECT * FROM public.inventory_reservations");
            jdbc.update("INSERT INTO tracing_upgrade.outbox_events (event_id, aggregate_id, event_type, event_version, occurred_at, correlation_id, causation_id, event_json, published_at, publication_attempts, next_attempt_at) SELECT event_id, aggregate_id, event_type, event_version, occurred_at, correlation_id, causation_id, event_json, published_at, publication_attempts, next_attempt_at FROM public.outbox_events");
            org.flywaydb.core.Flyway.configure().dataSource(dataSource)
                    .schemas("tracing_upgrade").defaultSchema("tracing_upgrade").load().migrate();
            assertThat(jdbc.queryForMap("SELECT event_id, event_json::text AS event_json, traceparent, correlation_id, causation_id, published_at, publication_attempts FROM tracing_upgrade.outbox_events WHERE correlation_id = ?", input.orderId())).isEqualTo(before);
            var column = jdbc.queryForMap("SELECT is_nullable, character_maximum_length FROM information_schema.columns WHERE table_schema = 'tracing_upgrade' AND table_name = 'outbox_events' AND column_name = 'traceparent'");
            assertThat(column.get("is_nullable")).isEqualTo("YES");
            assertThat(column.get("character_maximum_length")).isEqualTo(55);
        } finally { jdbc.execute("DROP SCHEMA tracing_upgrade CASCADE"); }
    }

    @Test void dltBrokerSuccessIsSeparateFromUnavailableCompletionMetadata() {
        var poison = new ConsumerRecord<byte[], byte[]>("warexis.order.events.v1", 0, offsets.incrementAndGet(), new byte[]{1}, new byte[]{2});
        var ack = mock(Acknowledgment.class);
        jdbc.execute("CREATE FUNCTION tracing_suppress_complete() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.phase = 'COMPLETE' THEN RETURN NULL; END IF; RETURN NEW; END $$");
        jdbc.execute("CREATE TRIGGER tracing_suppress_complete BEFORE UPDATE ON consumer_delivery_state FOR EACH ROW EXECUTE FUNCTION tracing_suppress_complete()");
        try { listener().receive(poison, ack, consumer()); }
        finally {
            jdbc.execute("DROP TRIGGER tracing_suppress_complete ON consumer_delivery_state");
            jdbc.execute("DROP FUNCTION tracing_suppress_complete()");
        }
        assertThat(attempts("warexis.kafka.dlt")).hasSize(1).allSatisfy(this::assertSuccess);
        assertThat(delivery.load(poison).phase()).isEqualTo("DLT");
        assertThat(delivery.load(poison).dltAttempts()).isEqualTo(1);
        verify(ack, never()).acknowledge();
        verify(transport, times(1)).send(any(ProducerRecord.class));
    }

    private OrderPlaced input() {
        var sku = "trace-sku-" + UUID.randomUUID();
        stocks.create(sku, 5);
        return new OrderPlaced(UUID.randomUUID(), UUID.randomUUID(), List.of(new ReservationLine(UUID.randomUUID(), sku, 1)));
    }
    private ConsumerRecord<byte[], byte[]> record(OrderPlaced input, String parent) {
        var json = mapper.createObjectNode();
        json.put("eventId", input.eventId().toString()); json.put("eventType", "OrderPlaced"); json.put("eventVersion", 1);
        json.put("occurredAt", "2026-10-06T09:00:00Z"); json.put("aggregateId", input.orderId().toString()); json.put("correlationId", input.orderId().toString());
        var payload = json.putObject("payload"); payload.put("orderId", input.orderId().toString()); payload.set("lines", mapper.valueToTree(input.lines()));
        var record = new ConsumerRecord<byte[], byte[]>("warexis.order.events.v1", 0, offsets.incrementAndGet(), input.orderId().toString().getBytes(StandardCharsets.UTF_8), mapper.writeValueAsBytes(json));
        if (parent != null) record.headers().add("traceparent", parent.getBytes(StandardCharsets.US_ASCII));
        return record;
    }
    private Map<String, Object> row(UUID orderId) { return jdbc.queryForMap("SELECT event_id, event_json::text AS event_json, traceparent, correlation_id, causation_id, published_at, publication_attempts FROM outbox_events WHERE correlation_id = ?", orderId); }
    private InventoryOutboxPublisher publisher() { return new InventoryOutboxPublisher(dataSource, transport, properties, validator, InventoryMetrics.disabled(), tracing); }
    private InventoryOrderListener listener() { return new InventoryOrderListener(delivery, validator, reservations, transport, InventoryMetrics.disabled(), tracing); }
    @SuppressWarnings("unchecked") private Consumer<byte[], byte[]> consumer() { return mock(Consumer.class); }
    private List<SpanData> attempts(String name) { return spans.stream().filter(s -> s.getName().equals(name)).toList(); }
    private String context(SpanData span) { return "00-" + span.getTraceId() + "-" + span.getSpanId() + (span.getSpanContext().isSampled() ? "-01" : "-00"); }
    private void assertSuccess(SpanData span) { assertThat(span.getAttributes().get(AttributeKey.stringKey("warexis.outcome"))).isEqualTo("success"); assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.UNSET); }
    private void assertFailure(SpanData span) { assertThat(span.getAttributes().get(AttributeKey.stringKey("warexis.outcome"))).isEqualTo("failure"); assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR); }
}
