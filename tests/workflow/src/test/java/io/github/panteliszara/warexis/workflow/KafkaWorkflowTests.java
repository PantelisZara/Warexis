package io.github.panteliszara.warexis.workflow;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

/** Black-box service processes with test-owned SQL fault injection and explicit recovery. */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Timeout(180)
class KafkaWorkflowTests {
    private static final String ORDER_TOPIC = "warexis.order.events.v1";
    private static final String INVENTORY_TOPIC = "warexis.inventory.events.v1";
    private static final String INVENTORY_GROUP = "warexis.inventory.order-events.v1";
    private static final String ORDER_GROUP = "warexis.order.inventory-events.v1";
    private static final String UNSAFE_MARKER = "WORKFLOW_UNSAFE_PAYLOAD_MARKER";
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.2.2");
    private static final PostgreSQLContainer ORDER_DB = new PostgreSQLContainer("postgres:18.6");
    private static final PostgreSQLContainer INVENTORY_DB = new PostgreSQLContainer("postgres:18.6");
    private static Process orderProcess;
    private static Process inventoryProcess;
    private static int orderPort;
    private static int inventoryPort;
    private static AdminClient admin;
    private static KafkaProducer<String, byte[]> producer;
    private static UUID sampleOrder;

    @BeforeAll
    static void start() throws Exception {
        try {
            requireJar("order");
            requireJar("inventory");
            // Keep restart diagnostics within this run; previous runs must not satisfy log assertions.
            var target = Path.of("target");
            Files.createDirectories(target);
            for (var service : List.of("order", "inventory")) Files.writeString(target.resolve(service + ".log"), "");
            ORDER_DB.start();
            INVENTORY_DB.start();
            KAFKA.start();
            admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers(), "request.timeout.ms", "10000"));
            admin.createTopics(List.of(new NewTopic(ORDER_TOPIC, 3, (short) 1), new NewTopic(INVENTORY_TOPIC, 3, (short) 1),
                    new NewTopic(ORDER_TOPIC + ".inventory.dlt", 3, (short) 1),
                    new NewTopic(INVENTORY_TOPIC + ".order.dlt", 3, (short) 1))).all().get(30, TimeUnit.SECONDS);
            var properties = new Properties();
            properties.put("bootstrap.servers", KAFKA.getBootstrapServers());
            properties.put("key.serializer", StringSerializer.class.getName());
            properties.put("value.serializer", ByteArraySerializer.class.getName());
            properties.put("acks", "all");
            properties.put("delivery.timeout.ms", "10000");
            properties.put("request.timeout.ms", "5000");
            properties.put("max.block.ms", "10000");
            producer = new KafkaProducer<>(properties);
            orderPort = freePort();
            do { inventoryPort = freePort(); } while (inventoryPort == orderPort);
            inventoryProcess = startService("inventory", inventoryPort, INVENTORY_DB);
            orderProcess = startService("order", orderPort, ORDER_DB);
            health(inventoryPort, inventoryProcess);
            health(orderPort, orderProcess);
        } catch (Throwable failure) {
            try { stop(); } catch (Throwable cleanupFailure) { failure.addSuppressed(cleanupFailure); }
            throw failure;
        }
    }

    @AfterAll
    static void stop() throws Exception {
        var failures = new ArrayList<Throwable>();
        cleanup(() -> stopProcess(orderProcess), failures);
        cleanup(() -> stopProcess(inventoryProcess), failures);
        cleanup(() -> { if (producer != null) producer.close(Duration.ofSeconds(5)); }, failures);
        cleanup(() -> { if (admin != null) admin.close(Duration.ofSeconds(5)); }, failures);
        cleanup(KAFKA::stop, failures);
        cleanup(INVENTORY_DB::stop, failures);
        cleanup(ORDER_DB::stop, failures);
        if (!failures.isEmpty()) {
            var failure = new AssertionError("Owned workflow resources could not all be cleaned up");
            failures.forEach(failure::addSuppressed);
            throw failure;
        }
    }

    @Test @Order(1)
    void confirmsOriginalRepeatedLinesWithOneDurableOutcome() throws Exception {
        var sku = stock(10);
        var second = stock(5);
        sampleOrder = createOrder(List.of(line(sku, 2), line(second, 1), line(sku, 3)));
        terminal(sampleOrder, "CONFIRMED");
        assertReserved(sku, 5);
        assertReserved(second, 1);
        assertDecision(sampleOrder, "RESERVED");
        assertThat(count(ORDER_DB, "SELECT count(*) FROM outbox_events WHERE aggregate_id = ?", sampleOrder)).isEqualTo(1);
        assertThat(count(INVENTORY_DB, "SELECT count(*) FROM outbox_events WHERE correlation_id = ?", sampleOrder)).isEqualTo(1);
        var placed = JSON.readTree(event(ORDER_DB, sampleOrder, "aggregate_id"));
        var reserved = JSON.readTree(event(INVENTORY_DB, sampleOrder, "correlation_id"));
        assertThat(reserved.path("payload").path("lines")).isEqualTo(placed.path("payload").path("lines"));
        assertThat(reserved.path("causationId").asString()).isEqualTo(placed.path("eventId").asString());
        await(() -> count(ORDER_DB, "SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND published_at IS NOT NULL", sampleOrder) == 1
                && count(INVENTORY_DB, "SELECT count(*) FROM outbox_events WHERE correlation_id = ? AND published_at IS NOT NULL", sampleOrder) == 1, "both outboxes published");
    }

    @Test @Order(2)
    void insufficientStockRejectsAllLinesWithoutPartialReservation() throws Exception {
        var enough = stock(10);
        var scarce = stock(1);
        var id = createOrder(List.of(line(enough, 2), line(scarce, 2)));
        terminal(id, "REJECTED");
        assertReserved(enough, 0);
        assertReserved(scarce, 0);
        assertDecision(id, "REJECTED");
        var rejectedSource = source(ORDER_TOPIC, id);
        committed(rejectedSource, INVENTORY_GROUP);
        assertThat(deliveryAttempts("inventory", rejectedSource, false)).isEqualTo(1);
        assertThat(deliveryAttempts("inventory", rejectedSource, true)).isZero();
        assertThat(deliveryPhase("inventory", rejectedSource)).isEqualTo("COMPLETE");
        assertThat(JSON.readTree(event(INVENTORY_DB, id, "correlation_id")).path("payload").path("reasonCode").asString()).isEqualTo("INSUFFICIENT_STOCK");
    }

    @Test @Order(3)
    void unknownSkuRejectsKnownStockWithoutReservingIt() throws Exception {
        var known = stock(10);
        var id = createOrder(List.of(line(known, 2), line("Missing-" + UUID.randomUUID(), 1)));
        terminal(id, "REJECTED");
        assertReserved(known, 0);
        assertThat(JSON.readTree(event(INVENTORY_DB, id, "correlation_id")).path("payload").path("reasonCode").asString()).isEqualTo("UNKNOWN_SKU");
    }

    @Test @Order(4)
    void concurrentOrdersCannotOversellThroughRealKafka() throws Exception {
        var sku = stock(5);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var a = executor.submit(() -> createOrder(List.of(line(sku, 4))));
            var b = executor.submit(() -> createOrder(List.of(line(sku, 4))));
            var first = a.get(15, TimeUnit.SECONDS);
            var second = b.get(15, TimeUnit.SECONDS);
            await(() -> !status(first).equals("PENDING_INVENTORY") && !status(second).equals("PENDING_INVENTORY"), "both competing orders terminal");
            assertThat(List.of(status(first), status(second))).containsExactlyInAnyOrder("CONFIRMED", "REJECTED");
            assertReserved(sku, 4);
        }
    }

    @Test @Order(5)
    void duplicateDeliveryInBothDirectionsPreservesDecisionAndStock() throws Exception {
        var sku = stock(10);
        var id = createOrder(List.of(line(sku, 3)));
        terminal(id, "CONFIRMED");
        var placed = event(ORDER_DB, id, "aggregate_id");
        var outcome = event(INVENTORY_DB, id, "correlation_id");
        var reservation = JSON.readTree(outcome).path("payload").path("reservationId").asString();
        committed(send(ORDER_TOPIC, id.toString(), placed), INVENTORY_GROUP);
        committed(send(INVENTORY_TOPIC, id.toString(), outcome), ORDER_GROUP);
        assertReserved(sku, 3);
        assertThat(status(id)).isEqualTo("CONFIRMED");
        assertThat(count(INVENTORY_DB, "SELECT count(*) FROM inventory_reservations WHERE order_id = ?", id)).isEqualTo(1);
        assertThat(count(INVENTORY_DB, "SELECT count(*) FROM outbox_events WHERE correlation_id = ?", id)).isEqualTo(1);
        assertThat(JSON.readTree(event(INVENTORY_DB, id, "correlation_id")).path("payload").path("reservationId").asString()).isEqualTo(reservation);
    }

    static Stream<String> poisonCases() { return Stream.of("malformed", "version", "key", "utf8", "trailing", "duplicate", "quantity"); }

    @ParameterizedTest @MethodSource("poisonCases") @Order(6)
    void inventoryQuarantinesPoisonWithoutMutationAndCommitsSource(String kind) throws Exception {
        poison(ORDER_TOPIC, INVENTORY_GROUP, ORDER_TOPIC + ".inventory.dlt", kind, event(ORDER_DB, sampleOrder, "aggregate_id"));
    }

    @ParameterizedTest @MethodSource("poisonCases") @Order(7)
    void orderQuarantinesPoisonWithoutMutationAndCommitsSource(String kind) throws Exception {
        poison(INVENTORY_TOPIC, ORDER_GROUP, INVENTORY_TOPIC + ".order.dlt", kind, event(INVENTORY_DB, sampleOrder, "correlation_id"));
    }

    @Test @Order(8)
    void brokerOutagePersistsPublicationPauseAcrossProcessRestartAndExplicitRecovery() throws Exception {
        var sku = stock(10);
        var docker = DockerClientFactory.instance().client();
        docker.pauseContainerCmd(KAFKA.getContainerId()).exec();
        UUID id;
        String original;
        try {
            id = createOrder(List.of(line(sku, 2)));
            original = event(ORDER_DB, id, "aggregate_id");
            await(() -> count(ORDER_DB, "SELECT count(*) FROM messaging_relay_control WHERE id = 1 AND paused", null) == 1, "relay reaches durable bounded pause", Duration.ofSeconds(90));
            assertThat(count(ORDER_DB, "SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND published_at IS NULL AND publication_attempts = 5", id)).isEqualTo(1);
        } finally {
            docker.unpauseContainerCmd(KAFKA.getContainerId()).exec();
        }
        stopProcess(orderProcess);
        orderProcess = startService("order", orderPort, ORDER_DB);
        health(orderPort, orderProcess);
        assertThat(count(ORDER_DB, "SELECT count(*) FROM messaging_relay_control WHERE id = 1 AND paused", null)).isEqualTo(1);
        assertThat(event(ORDER_DB, id, "aggregate_id")).isEqualTo(original);
        assertThat(status(id)).isEqualTo("PENDING_INVENTORY");
        // Stop the owned service before resetting its disposable database, as operators do.
        stopProcess(orderProcess);
        execute(ORDER_DB, "UPDATE outbox_events SET publication_attempts = 0, next_attempt_at = NULL WHERE published_at IS NULL");
        execute(ORDER_DB, "UPDATE messaging_relay_control SET paused = false, failure_classification = NULL WHERE id = 1");
        orderProcess = startService("order", orderPort, ORDER_DB);
        health(orderPort, orderProcess);
        terminal(id, "CONFIRMED");
        assertReserved(sku, 2);
        assertThat(event(ORDER_DB, id, "aggregate_id")).isEqualTo(original);
    }

    static Stream<String> serviceDirections() { return Stream.of("order", "inventory"); }

    @ParameterizedTest(name = "{0}: pending outbox survives a JVM crash")
    @MethodSource("serviceDirections") @Order(9)
    void pendingOutboxResumesAfterActualProcessRestart(String service) throws Exception {
        var sku = stock(4);
        var database = database(service);
        inject(database, "outbox_events", "NEW.publication_attempts > OLD.publication_attempts", false);
        UUID id;
        String original;
        try {
            id = createOrder(List.of(line(sku, 2)));
            await(() -> count(database, "SELECT count(*) FROM outbox_events WHERE " + eventColumn(service) + " = ?", id) == 1,
                    service + " committed pending outbox");
            original = event(database, id, eventColumn(service));
            assertThat(count(database, "SELECT count(*) FROM outbox_events WHERE " + eventColumn(service)
                    + " = ? AND published_at IS NULL AND publication_attempts = 0", id)).isEqualTo(1);
            if (service.equals("inventory")) assertReserved(sku, 2);
            crashService(service);
        } finally { removeInjection(database, "outbox_events"); }
        restartService(service);
        terminal(id, "CONFIRMED");
        await(() -> count(database, "SELECT count(*) FROM outbox_events WHERE " + eventColumn(service)
                + " = ? AND published_at IS NOT NULL", id) == 1, service + " pending publication resumes");
        assertThat(event(database, id, eventColumn(service))).isEqualTo(original);
        assertReserved(sku, 2);
        assertOneDecision(id);
    }

    @ParameterizedTest(name = "{0}: restart after business commit before delivery checkpoint")
    @MethodSource("serviceDirections") @Order(10)
    void consumerCrashAfterBusinessCommitReplaysWithoutDuplicateEffect(String service) throws Exception {
        var sku = stock(4);
        var database = database(service);
        inject(database, deliveryTable(service), "NEW.phase = 'COMPLETE'", true);
        UUID id;
        Source captured;
        String outcome;
        try {
            id = createOrder(List.of(line(sku, 2)));
            captured = source(sourceTopic(service), id);
            await(() -> deliveryPhase(service, captured).equals("PROCESS")
                    && deliveryAttempts(service, captured, false) == 1
                    && count(database, "SELECT count(*) FROM " + (service.equals("order") ? "inventory_decisions" : "inventory_reservations")
                    + " WHERE order_id = ?", id) == 1,
                    service + " business commit precedes failed delivery checkpoint");
            if (service.equals("order")) assertThat(status(id)).isEqualTo("CONFIRMED");
            assertReserved(sku, 2);
            assertUncommitted(captured, group(service));
            outcome = event(INVENTORY_DB, id, "correlation_id");
            var diagnosticSource = captured;
            var diagnosticJson = service.equals("order") ? outcome : event(ORDER_DB, id, "aggregate_id");
            diagnostic(service, diagnosticSource, "PAUSED", diagnosticJson, 1);
            crashService(service);
        } finally { removeInjection(database, deliveryTable(service)); }
        restartService(service);
        committed(captured, group(service));
        terminal(id, "CONFIRMED");
        assertThat(deliveryPhase(service, captured)).isEqualTo("COMPLETE");
        assertThat(deliveryAttempts(service, captured, false)).isEqualTo(2);
        assertReserved(sku, 2);
        assertOneDecision(id);
        assertThat(event(INVENTORY_DB, id, "correlation_id")).isEqualTo(outcome);
    }

    @ParameterizedTest(name = "{0}: unavailable owned database does not acknowledge source")
    @MethodSource("serviceDirections") @Order(11)
    void temporaryDatabaseOutageRequiresSafeProcessRecovery(String service) throws Exception {
        var sku = stock(4);
        var database = database(service);
        var docker = DockerClientFactory.instance().client();
        UUID id = null;
        Source captured = null;
        // Capture the healthy business marker before the outage. Kafka offset observations remain independent of JDBC.
        if (service.equals("order")) {
            stopProcess(inventoryProcess);
            id = createOrder(List.of(line(sku, 2)));
            assertThat(status(id)).isEqualTo("PENDING_INVENTORY");
            source(ORDER_TOPIC, id); // Broker acceptance is required before disabling the Order database.
        }
        docker.pauseContainerCmd(database.getContainerId()).exec();
        try {
            if (service.equals("inventory")) id = createOrder(List.of(line(sku, 2)));
            else restartService("inventory");
            captured = source(sourceTopic(service), id);
            var blockedSource = captured;
            observeBlocked(() -> assertUncommitted(blockedSource, group(service)), service + " unavailable database");
            crashService(service);
        } finally { docker.unpauseContainerCmd(database.getContainerId()).exec(); }
        restartService(service);
        committed(captured, group(service));
        terminal(id, "CONFIRMED");
        assertThat(deliveryPhase(service, captured)).isEqualTo("COMPLETE");
        assertThat(deliveryAttempts(service, captured, false)).isBetween(1L, 4L);
        assertReserved(sku, 2);
        assertOneDecision(id);
    }

    @ParameterizedTest(name = "{0}: exhausted DLT blocks its partition across JVM restart")
    @MethodSource("serviceDirections") @Order(12) @Timeout(240)
    void exhaustedDltPreservesPartitionOrderingAcrossRestartAndExplicitRecovery(String service) throws Exception {
        var database = database(service);
        var dlt = sourceTopic(service) + (service.equals("order") ? ".order.dlt" : ".inventory.dlt");
        var resource = new ConfigResource(ConfigResource.Type.TOPIC, dlt);
        var originalLimit = admin.describeConfigs(List.of(resource)).all().get(10, TimeUnit.SECONDS)
                .get(resource).get("max.message.bytes").value();
        var key = UUID.randomUUID();
        int blockedPartition = partition(key);
        var poison = ("{" + UNSAFE_MARKER + "\n" + "x".repeat(5000)).getBytes(StandardCharsets.UTF_8);
        var later = new ArrayList<UUID>();
        var laterSources = new java.util.LinkedHashMap<UUID, Source>();
        var samePartition = new ArrayList<UUID>();
        var otherPartition = new ArrayList<UUID>();
        var sku = stock(1000);
        Source blocked = null;
        try {
            topicLimit(resource, "1024");
            blocked = Source.of(send(sourceTopic(service), key.toString(), poison));
            var source = blocked;
            assertThat(source.partition()).isEqualTo(blockedPartition);
            await(() -> deliveryPhase(service, source).equals("PAUSED"), service + " four failed real DLT sends exhaust durable budget", Duration.ofSeconds(75));
            assertThat(deliveryAttempts(service, source, true)).isEqualTo(4);
            for (int attempt = 1; attempt <= 4; attempt++) diagnostic(service, source, "DLT_RETRY", null, attempt);
            diagnostic(service, source, "PAUSED", null, 4);
            assertUncommitted(source, group(service));
            for (int i = 0; i < 96 && (samePartition.size() < 3 || otherPartition.isEmpty()); i++) {
                var id = createOrder(List.of(line(sku, 1)));
                later.add(id);
                (partition(id) == blockedPartition ? samePartition : otherPartition).add(id);
            }
            assertThat(samePartition).as("at least three distinct orderIds on the blocked partition").hasSizeGreaterThanOrEqualTo(3);
            assertThat(otherPartition).isNotEmpty();
            for (var id : otherPartition) terminal(id, "CONFIRMED");
            for (var id : later) laterSources.put(id, source(sourceTopic(service), id));
            observeBlocked(() -> assertPartitionBlocked(service, source, samePartition), service + " repeated polls while another partition progresses");
            crashService(service);
            restartService(service);
            // A new other-partition record proves the restarted consumer is active during the blocked observation.
            UUID restartMarker = null;
            for (int i = 0; i < 96 && restartMarker == null; i++) {
                var id = createOrder(List.of(line(sku, 1)));
                later.add(id);
                laterSources.put(id, source(sourceTopic(service), id));
                if (partition(id) == blockedPartition) samePartition.add(id);
                else { restartMarker = id; terminal(id, "CONFIRMED"); committed(laterSources.get(id), group(service)); }
            }
            assertThat(restartMarker).as("new other-partition marker after restart").isNotNull();
            observeBlocked(() -> assertPartitionBlocked(service, source, samePartition), service + " durable pause after actual JVM restart");
            assertThat(deliveryAttempts(service, source, true)).isEqualTo(4);
            // Follow the operator procedure: stop the affected process before resetting only the diagnosed record.
            stopService(service);
            topicLimit(resource, originalLimit);
            execute(database, "UPDATE " + deliveryTable(service) + " SET phase = 'DLT', dlt_attempts = 0, next_attempt_at = NULL WHERE source_topic = '"
                    + source.topic() + "' AND source_partition = " + source.partition() + " AND source_offset = " + source.offset() + " AND phase = 'PAUSED'");
            restartService(service);
            committed(source, group(service));
            for (var id : later) { terminal(id, "CONFIRMED"); committed(laterSources.get(id), group(service)); }
            assertThat(deliveryPhase(service, source)).isEqualTo("COMPLETE");
            assertThat(deliveryAttempts(service, source, true)).isEqualTo(1);
            assertThat(dltCopies(dlt, source, poison)).isEqualTo(1);
            var dltEnd = admin.listOffsets(Map.of(new TopicPartition(dlt, blockedPartition),
                    org.apache.kafka.clients.admin.OffsetSpec.latest())).all().get(10, TimeUnit.SECONDS)
                    .get(new TopicPartition(dlt, blockedPartition)).offset();
            crashService(service);
            restartService(service);
            // Prove the previously blocked partition is actively consuming after the completed DLT restart.
            UUID completedMarker = null;
            for (int i = 0; i < 96 && completedMarker == null; i++) {
                var id = createOrder(List.of(line(sku, 1)));
                later.add(id);
                var next = source(sourceTopic(service), id);
                laterSources.put(id, next);
                terminal(id, "CONFIRMED");
                committed(next, group(service));
                if (partition(id) == blockedPartition) completedMarker = id;
            }
            assertThat(completedMarker).as("new same-partition marker after completed DLT restart").isNotNull();
            observeBlocked(() -> {
                assertThat(deliveryPhase(service, source)).isEqualTo("COMPLETE");
                assertThat(deliveryAttempts(service, source, true)).isEqualTo(1);
                assertThat(offsetEnd(dlt, blockedPartition)).isEqualTo(dltEnd);
                for (var id : later) { assertThat(status(id)).isEqualTo("CONFIRMED"); assertOneDecision(id); }
            }, service + " completed DLT cannot resurrect after restart");
            assertReserved(sku, later.size());
            noUnsafePayloadLogs();
        } finally { topicLimit(resource, originalLimit); }
    }

    private record Source(String topic, int partition, long offset) {
        static Source of(RecordMetadata record) { return new Source(record.topic(), record.partition(), record.offset()); }
    }
    private static PostgreSQLContainer database(String service) { return service.equals("order") ? ORDER_DB : INVENTORY_DB; }
    private static String deliveryTable(String service) { return service.equals("order") ? "messaging_deliveries" : "consumer_delivery_state"; }
    private static String eventColumn(String service) { return service.equals("order") ? "aggregate_id" : "correlation_id"; }
    private static String sourceTopic(String service) { return service.equals("order") ? INVENTORY_TOPIC : ORDER_TOPIC; }
    private static String group(String service) { return service.equals("order") ? ORDER_GROUP : INVENTORY_GROUP; }
    private static int partition(UUID id) { return org.apache.kafka.common.utils.Utils.toPositive(org.apache.kafka.common.utils.Utils.murmur2(id.toString().getBytes(StandardCharsets.UTF_8))) % 3; }
    private static void stopService(String service) throws Exception { stopProcess(service.equals("order") ? orderProcess : inventoryProcess); }
    private static void crashService(String service) throws Exception {
        var process = service.equals("order") ? orderProcess : inventoryProcess;
        process.destroyForcibly();
        assertThat(process.waitFor(10, TimeUnit.SECONDS)).as("owned %s JVM terminated", service).isTrue();
    }
    private static void restartService(String service) throws Exception {
        stopService(service);
        if (service.equals("order")) { orderProcess = startService(service, orderPort, ORDER_DB); health(orderPort, orderProcess); }
        else { inventoryProcess = startService(service, inventoryPort, INVENTORY_DB); health(inventoryPort, inventoryProcess); }
    }
    private static void assertOneDecision(UUID id) {
        assertThat(count(INVENTORY_DB, "SELECT count(*) FROM inventory_reservations WHERE order_id = ?", id)).isEqualTo(1);
        assertThat(count(INVENTORY_DB, "SELECT count(*) FROM outbox_events WHERE correlation_id = ?", id)).isEqualTo(1);
        assertThat(count(ORDER_DB, "SELECT count(*) FROM inventory_decisions WHERE order_id = ?", id)).isEqualTo(1);
    }
    private static String sourcePredicate(Source source) {
        return "source_topic = '" + source.topic() + "' AND source_partition = " + source.partition() + " AND source_offset = " + source.offset();
    }
    private static String deliveryPhase(String service, Source source) {
        try (var connection = DriverManager.getConnection(database(service).getJdbcUrl(), database(service).getUsername(), database(service).getPassword());
             var query = connection.createStatement(); var row = query.executeQuery("SELECT phase FROM " + deliveryTable(service) + " WHERE " + sourcePredicate(source))) {
            return row.next() ? row.getString(1) : "ABSENT";
        } catch (Exception failure) { throw new IllegalStateException("Delivery observation failed", failure); }
    }
    private static long deliveryAttempts(String service, Source source, boolean dlt) {
        return count(database(service), "SELECT " + (dlt ? "dlt_attempts" : "processing_attempts") + " FROM " + deliveryTable(service) + " WHERE " + sourcePredicate(source), null);
    }
    private static Source source(String topic, UUID id) throws Exception {
        try (var observer = dltConsumer(topic)) {
            var partitions = List.of(new TopicPartition(topic, 0), new TopicPartition(topic, 1), new TopicPartition(topic, 2));
            observer.assign(partitions); observer.seekToBeginning(partitions);
            var deadline = System.nanoTime() + Duration.ofSeconds(40).toNanos();
            while (System.nanoTime() < deadline) for (var record : observer.poll(Duration.ofMillis(200))) {
                if (id.toString().equals(record.key())) {
                    assertThat(record.partition()).as("orderId key maps to its Kafka partition").isEqualTo(partition(id));
                    return new Source(topic, record.partition(), record.offset());
                }
            }
        }
        throw new AssertionError("Source record missing for " + topic + " order " + id);
    }
    private static void assertUncommitted(Source source, String group) {
        try {
            var offset = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS)
                    .get(new TopicPartition(source.topic(), source.partition()));
            assertThat(offset == null || offset.offset() <= source.offset()).as("%s source offset %s remains uncommitted", group, source).isTrue();
        } catch (Exception failure) { throw new IllegalStateException("Kafka offset observation failed", failure); }
    }
    private static void committed(Source source, String group) throws Exception {
        await(() -> { try { var offset = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS)
                .get(new TopicPartition(source.topic(), source.partition())); return offset != null && offset.offset() > source.offset(); }
                catch (Exception failure) { return false; } }, "source offset committed after process recovery");
    }
    private static void observeBlocked(Runnable assertion, String description) throws Exception {
        var deadline = System.nanoTime() + Duration.ofMillis(1500).toNanos();
        do {
            try { assertion.run(); } catch (AssertionError failure) { throw new AssertionError(description, failure); }
            Thread.sleep(100);
        } while (System.nanoTime() < deadline);
    }
    private static void assertPartitionBlocked(String service, Source source, List<UUID> ids) {
        assertUncommitted(source, group(service));
        assertThat(deliveryPhase(service, source)).isEqualTo("PAUSED");
        for (var id : ids) {
            assertThat(status(id)).isEqualTo("PENDING_INVENTORY");
            assertThat(count(database(service), "SELECT count(*) FROM " + (service.equals("order") ? "inventory_decisions" : "inventory_reservations") + " WHERE order_id = ?", id)).isZero();
        }
    }
    private static void inject(PostgreSQLContainer db, String table, String predicate, boolean suppress) throws Exception {
        execute(db, "CREATE FUNCTION workflow_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF " + predicate
                + " THEN " + (suppress ? "RETURN NULL;" : "RAISE EXCEPTION 'test publication unavailable';") + " END IF; RETURN NEW; END $$");
        execute(db, "CREATE TRIGGER workflow_fault BEFORE UPDATE ON " + table + " FOR EACH ROW EXECUTE FUNCTION workflow_fault()");
    }
    private static void removeInjection(PostgreSQLContainer db, String table) throws Exception {
        execute(db, "DROP TRIGGER workflow_fault ON " + table);
        execute(db, "DROP FUNCTION workflow_fault()");
    }
    private static void topicLimit(ConfigResource resource, String limit) throws Exception {
        admin.incrementalAlterConfigs(Map.of(resource, List.of(new AlterConfigOp(new ConfigEntry("max.message.bytes", limit), AlterConfigOp.OpType.SET))))
                .all().get(10, TimeUnit.SECONDS);
    }
    private static long offsetEnd(String topic, int partition) {
        try { return admin.listOffsets(Map.of(new TopicPartition(topic, partition), org.apache.kafka.clients.admin.OffsetSpec.latest()))
                .all().get(10, TimeUnit.SECONDS).get(new TopicPartition(topic, partition)).offset(); }
        catch (Exception failure) { throw new IllegalStateException("Kafka end offset observation failed", failure); }
    }
    private static void diagnostic(String service, Source source, String state, String validJson, int attempt) throws Exception {
        var expected = validJson == null ? null : JSON.readTree(validJson);
        await(() -> {
            try {
                return Files.readAllLines(Path.of("target", service + ".log")).stream().anyMatch(line ->
                        field(line, "state", state) && field(line, "partition", Integer.toString(source.partition()))
                        && field(line, "offset", Long.toString(source.offset()))
                        && field(line, service.equals("order") ? "attempt" : state.equals("PAUSED") && expected != null ? "processingAttempt" : "dltAttempt", Integer.toString(attempt))
                        && field(line, "eventId", expected == null ? "null" : expected.path("eventId").asString())
                        && field(line, "eventType", expected == null ? "null" : expected.path("eventType").asString())
                        && field(line, "orderId", expected == null ? "null" : expected.path("payload").path("orderId").asString()));
            } catch (Exception failure) { throw new IllegalStateException("Service diagnostic observation failed", failure); }
        }, service + " safe " + state + " diagnostic for " + source);
        noUnsafePayloadLogs();
    }
    private static boolean field(String line, String name, String value) {
        return java.util.regex.Pattern.compile("(?:^|[ ,])" + name + "=" + java.util.regex.Pattern.quote(value) + "(?:[ ,]|$)").matcher(line).find();
    }
    private static void noUnsafePayloadLogs() throws Exception {
        for (var service : List.of("order", "inventory")) {
            assertThat(Files.readString(Path.of("target", service + ".log")))
                    .as("%s logs omit unsafe poison bytes", service).doesNotContain(UNSAFE_MARKER);
        }
    }
    private static long dltCopies(String topic, Source source, byte[] expected) throws Exception {
        long copies = 0;
        try (var observer = dltConsumer(topic)) {
            var partition = new TopicPartition(topic, source.partition());
            observer.assign(List.of(partition)); observer.seekToBeginning(List.of(partition));
            var end = offsetEnd(topic, source.partition());
            var deadline = System.nanoTime() + Duration.ofSeconds(40).toNanos();
            while (observer.position(partition) < end && System.nanoTime() < deadline) for (var record : observer.poll(Duration.ofMillis(200))) {
                if (header(record, "warexis-source-offset").equals(Long.toString(source.offset()))
                        && header(record, "warexis-source-topic").equals(source.topic())) {
                    assertThat(record.value()).containsExactly(expected);
                    assertThat(header(record, "warexis-dlt-attempts")).isEqualTo("1");
                    copies++;
                }
            }
            assertThat(observer.position(partition)).as("DLT observer reaches captured end").isGreaterThanOrEqualTo(end);
        }
        return copies;
    }

    private void poison(String topic, String group, String dlt, String kind, String valid) throws Exception {
        var orderCount = count(ORDER_DB, "SELECT count(*) FROM orders", null);
        var reservationCount = count(INVENTORY_DB, "SELECT count(*) FROM inventory_reservations", null);
        var orderEvents = count(ORDER_DB, "SELECT count(*) FROM outbox_events", null);
        var inventoryEvents = count(INVENTORY_DB, "SELECT count(*) FROM outbox_events", null);
        var stockRows = count(INVENTORY_DB, "SELECT count(*) FROM stock_items", null);
        var reservedTotal = count(INVENTORY_DB, "SELECT COALESCE(sum(reserved), 0) FROM stock_items", null);
        var processedOrders = count(INVENTORY_DB, "SELECT count(*) FROM processed_order_events", null);
        var processedOutcomes = count(ORDER_DB, "SELECT count(*) FROM processed_inventory_events", null);
        var key = kind.equals("key") ? UUID.randomUUID().toString() : sampleOrder.toString();
        byte[] value = switch (kind) {
            case "malformed" -> ("{" + UNSAFE_MARKER + "\n").getBytes(StandardCharsets.UTF_8);
            case "version" -> valid.replace("\"eventVersion\": 1", "\"eventVersion\": 99").replace("\"eventVersion\":1", "\"eventVersion\":99").getBytes(StandardCharsets.UTF_8);
            case "utf8" -> new byte[] {(byte) 0xC3, (byte) 0x28};
            case "quantity" -> valid.replaceAll("\"quantity\": [0-9]+", "\"quantity\": \"2\"").replaceAll("\"quantity\":[0-9]+", "\"quantity\":\"2\"").getBytes(StandardCharsets.UTF_8);
            case "trailing" -> (valid + " {}").getBytes(StandardCharsets.UTF_8);
            case "duplicate" -> valid.replace("\"eventVersion\": 1", "\"eventVersion\": 99, \"eventVersion\": 1")
                    .replace("\"eventVersion\":1", "\"eventVersion\":99,\"eventVersion\":1").getBytes(StandardCharsets.UTF_8);
            default -> valid.getBytes(StandardCharsets.UTF_8);
        };
        var metadata = send(topic, key, value);
        try (var consumer = dltConsumer(dlt)) {
            var partitions = List.of(new TopicPartition(dlt, 0), new TopicPartition(dlt, 1), new TopicPartition(dlt, 2));
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            var found = false;
            var deadline = System.nanoTime() + Duration.ofSeconds(40).toNanos();
            while (!found && System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(200))) {
                    if (key.equals(record.key()) && java.util.Arrays.equals(value, record.value())) {
                        assertThat(record.partition()).isEqualTo(metadata.partition());
                        assertThat(header(record, "warexis-source-topic")).isEqualTo(metadata.topic());
                        assertThat(header(record, "warexis-source-partition")).isEqualTo(Integer.toString(metadata.partition()));
                        assertThat(header(record, "warexis-source-offset")).isEqualTo(Long.toString(metadata.offset()));
                        assertThat(header(record, "warexis-consumer-group")).isEqualTo(group);
                        assertThat(header(record, "warexis-failure-code")).isNotBlank();
                        assertThat(Integer.parseInt(header(record, "warexis-processing-attempts"))).isBetween(0, 4);
                        assertThat(Integer.parseInt(header(record, "warexis-dlt-attempts"))).isBetween(1, 4);
                        java.time.Instant.parse(header(record, "warexis-failed-at"));
                        found = true;
                    }
                }
            }
            assertThat(found).as("DLT retains original %s bytes", kind).isTrue();
        }
        committed(metadata, group);
        assertThat(count(ORDER_DB, "SELECT count(*) FROM orders", null)).isEqualTo(orderCount);
        assertThat(count(INVENTORY_DB, "SELECT count(*) FROM inventory_reservations", null)).isEqualTo(reservationCount);
        assertThat(count(ORDER_DB, "SELECT count(*) FROM outbox_events", null)).isEqualTo(orderEvents);
        assertThat(count(INVENTORY_DB, "SELECT count(*) FROM outbox_events", null)).isEqualTo(inventoryEvents);
        assertThat(count(INVENTORY_DB, "SELECT count(*) FROM stock_items", null)).isEqualTo(stockRows);
        assertThat(count(INVENTORY_DB, "SELECT COALESCE(sum(reserved), 0) FROM stock_items", null)).isEqualTo(reservedTotal);
        assertThat(count(INVENTORY_DB, "SELECT count(*) FROM processed_order_events", null)).isEqualTo(processedOrders);
        assertThat(count(ORDER_DB, "SELECT count(*) FROM processed_inventory_events", null)).isEqualTo(processedOutcomes);
        assertThat(status(sampleOrder)).isEqualTo("CONFIRMED");
        noUnsafePayloadLogs();
    }

    @FunctionalInterface private interface CleanupAction { void run() throws Exception; }
    private static void cleanup(CleanupAction action, List<Throwable> failures) {
        try { action.run(); } catch (Throwable failure) { failures.add(failure); }
    }

    private static Process startService(String name, int port, PostgreSQLContainer database) throws Exception {
        var target = Path.of("target").toAbsolutePath();
        Files.createDirectories(target);
        var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx384m", "-jar", requireJar(name).toString(),
                "--warexis.messaging.enabled=true", "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--warexis.messaging.retry-base-delay=10ms", "--warexis.messaging.poll-interval=50ms", "--server.address=127.0.0.1", "--server.port=" + port);
        builder.environment().put(name.toUpperCase() + "_DB_URL", database.getJdbcUrl());
        builder.environment().put(name.toUpperCase() + "_DB_USERNAME", database.getUsername());
        builder.environment().put(name.toUpperCase() + "_DB_PASSWORD", database.getPassword());
        return builder.redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.appendTo(target.resolve(name + ".log").toFile())).start();
    }

    private static Path requireJar(String name) {
        var path = Path.of(System.getProperty(name + ".jar", "../../services/" + name + "-service/target/" + name + "-service-0.0.1-SNAPSHOT.jar")).toAbsolutePath().normalize();
        if (!Files.isRegularFile(path)) throw new IllegalStateException("Build the packaged service first or provide -D" + name + ".jar");
        return path;
    }

    private static void stopProcess(Process process) throws Exception {
        if (process == null) return;
        process.destroy();
        if (!process.waitFor(10, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            assertThat(process.waitFor(5, TimeUnit.SECONDS)).as("owned process stopped").isTrue();
        }
    }

    private static void health(int port, Process process) throws Exception {
        await(() -> { assertThat(process.isAlive()).as("service process alive; inspect target service logs").isTrue(); return request(port, "/actuator/health", "GET", null).statusCode() == 200; }, "service health", Duration.ofSeconds(75));
    }

    private static int freePort() throws Exception { try (var socket = new ServerSocket(0)) { return socket.getLocalPort(); } }
    private static Map<String, Object> line(String sku, int quantity) { return Map.of("sku", sku, "quantity", quantity); }
    private static String stock(int onHand) {
        var sku = "SKU-" + UUID.randomUUID();
        var response = request(inventoryPort, "/api/stock-items", "POST", JSON.writeValueAsString(Map.of("sku", sku, "onHand", onHand)));
        assertThat(response.statusCode()).isEqualTo(201);
        return sku;
    }
    private static UUID createOrder(List<Map<String, Object>> lines) {
        var response = request(orderPort, "/api/orders", "POST", JSON.writeValueAsString(Map.of("lines", lines)));
        assertThat(response.statusCode()).isEqualTo(201);
        return UUID.fromString(JSON.readTree(response.body()).path("id").asString());
    }
    private static String status(UUID id) { return JSON.readTree(request(orderPort, "/api/orders/" + id, "GET", null).body()).path("status").asString(); }
    private static void terminal(UUID id, String expected) throws Exception { await(() -> status(id).equals(expected), "order " + id + " becomes " + expected); }
    private static void assertReserved(String sku, int expected) {
        var stock = JSON.readTree(request(inventoryPort, "/api/stock-items/" + sku, "GET", null).body());
        assertThat(stock.path("reserved").asInt()).isEqualTo(expected).isBetween(0, stock.path("onHand").asInt());
    }
    private static void assertDecision(UUID id, String expected) {
        assertThat(count(INVENTORY_DB, "SELECT count(*) FROM inventory_reservations WHERE order_id = ? AND status = '" + expected + "'", id)).isEqualTo(1);
    }
    private static HttpResponse<String> request(int port, String path, String method, String body) {
        try {
            return HTTP.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).timeout(Duration.ofSeconds(5)).header("Content-Type", "application/json")
                    .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
        } catch (Exception failure) { throw new IllegalStateException("HTTP request failed", failure); }
    }
    private static long count(PostgreSQLContainer database, String sql, Object id) {
        try (var connection = DriverManager.getConnection(database.getJdbcUrl(), database.getUsername(), database.getPassword()); var query = connection.prepareStatement(sql)) {
            if (id != null) query.setObject(1, id);
            try (var result = query.executeQuery()) { result.next(); return result.getLong(1); }
        } catch (Exception failure) { throw new IllegalStateException("SQL observation failed", failure); }
    }
    private static String event(PostgreSQLContainer database, UUID id, String column) {
        try (var connection = DriverManager.getConnection(database.getJdbcUrl(), database.getUsername(), database.getPassword()); var query = connection.prepareStatement("SELECT event_json::text FROM outbox_events WHERE " + column + " = ?")) {
            query.setObject(1, id);
            try (var result = query.executeQuery()) { assertThat(result.next()).isTrue(); return result.getString(1); }
        } catch (Exception failure) { throw new IllegalStateException("Event observation failed", failure); }
    }
    private static void execute(PostgreSQLContainer database, String sql) throws Exception { try (var connection = DriverManager.getConnection(database.getJdbcUrl(), database.getUsername(), database.getPassword()); var query = connection.createStatement()) { query.executeUpdate(sql); } }
    private static RecordMetadata send(String topic, String key, String value) throws Exception { return send(topic, key, value.getBytes(StandardCharsets.UTF_8)); }
    private static RecordMetadata send(String topic, String key, byte[] value) throws Exception { return producer.send(new ProducerRecord<>(topic, key, value)).get(15, TimeUnit.SECONDS); }
    private static void committed(RecordMetadata source, String group) throws Exception {
        var partition = new TopicPartition(source.topic(), source.partition());
        await(() -> { try { var offset = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS).get(partition); return offset != null && offset.offset() > source.offset(); } catch (Exception failure) { return false; } }, "source offset committed after processing/DLT");
    }
    private static String header(org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]> record, String name) {
        var header = record.headers().lastHeader(name);
        assertThat(header).as("DLT metadata %s", name).isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
    }
    private static KafkaConsumer<String, byte[]> dltConsumer(String topic) {
        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "workflow-observer-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(properties);
    }
    private static void await(BooleanSupplier condition, String description) throws Exception { await(condition, description, Duration.ofSeconds(45)); }
    private static void await(BooleanSupplier condition, String description, Duration timeout) throws Exception {
        var deadline = System.nanoTime() + timeout.toNanos();
        Throwable last = null;
        while (System.nanoTime() < deadline) {
            try { if (condition.getAsBoolean()) return; } catch (RuntimeException failure) { last = failure; }
            Thread.sleep(100);
        }
        throw new AssertionError("Timed out: " + description + "; inspect target/order.log and target/inventory.log", last);
    }
}
