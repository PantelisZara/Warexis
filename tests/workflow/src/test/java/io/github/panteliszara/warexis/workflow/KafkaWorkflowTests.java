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

/** Black-box service processes; SQL is used only to observe invariants or explicit recovery. */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Timeout(180)
class KafkaWorkflowTests {
    private static final String ORDER_TOPIC = "warexis.order.events.v1";
    private static final String INVENTORY_TOPIC = "warexis.inventory.events.v1";
    private static final String INVENTORY_GROUP = "warexis.inventory.order-events.v1";
    private static final String ORDER_GROUP = "warexis.order.inventory-events.v1";
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
            case "malformed" -> "{".getBytes(StandardCharsets.UTF_8);
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
