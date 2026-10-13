package io.github.panteliszara.warexis.inventory.stock;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@org.springframework.test.context.TestPropertySource(properties = {"WAREXIS_OAUTH2_ISSUER_URI=https://issuer.test/realms/warexis", "WAREXIS_OAUTH2_AUDIENCE=warexis-api"})
@org.springframework.context.annotation.Import(io.github.panteliszara.warexis.inventory.security.TestJwtConfiguration.class)
@Testcontainers
class StockItemIntegrationTests {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18.6");

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private DataSource dataSource;

    @LocalServerPort
    private int port;

    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test
    void createsReadsAndUpdatesWithStableIdentityAndDerivedAvailability() throws Exception {
        var sku = sku();
        var created = request("POST", "", "{\"sku\":\"" + sku + "\",\"onHand\":12}");
        assertThat(created.statusCode()).isEqualTo(201);
        assertThat(created.headers().firstValue("Location")).contains("/api/stock-items/" + sku);
        var stock = json(created);
        UUID.fromString(stock.path("id").asString());
        assertStock(stock, sku, 12, 0);
        var read = request("GET", "/" + sku, null);
        assertThat(read.statusCode()).isEqualTo(200);
        assertThat(json(read)).isEqualTo(stock);

        jdbc.update("UPDATE stock_items SET reserved = 5 WHERE sku = ?", sku);
        var updated = request("PUT", "/" + sku, "{\"onHand\":8}");
        assertThat(updated.statusCode()).isEqualTo(200);
        assertStock(json(updated), sku, 8, 5);
        assertThat(json(updated).path("id")).isEqualTo(stock.path("id"));
        assertThat(json(request("GET", "/" + sku, null))).isEqualTo(json(updated));
        assertThat(jdbc.queryForObject("SELECT count(*) FROM stock_items WHERE sku = ?", Integer.class, sku))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM information_schema.columns WHERE table_name = 'stock_items' AND column_name = 'available'", Integer.class))
                .isZero();
    }

    @Test
    void duplicateSkuConflictsButCaseDistinctSkuRemainsIndependent() throws Exception {
        var sku = "Case" + sku();
        var body = "{\"sku\":\"" + sku + "\",\"onHand\":1}";
        assertThat(request("POST", "", body).statusCode()).isEqualTo(201);
        assertProblem(request("POST", "", body), 409);
        assertThat(request("POST", "", "{\"sku\":\"" + sku.toLowerCase() + "\",\"onHand\":2}").statusCode())
                .isEqualTo(201);
        assertStock(json(request("GET", "/" + sku, null)), sku, 1, 0);
        assertStock(json(request("GET", "/" + sku.toLowerCase(), null)), sku.toLowerCase(), 2, 0);
    }

    @Test
    void missingReadsAndUpdatesAreNotFoundAndNeverUpsert() throws Exception {
        var sku = sku();
        assertProblem(request("GET", "/" + sku, null), 404);
        assertProblem(request("PUT", "/" + sku, "{\"onHand\":1}"), 404);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM stock_items WHERE sku = ?", Integer.class, sku))
                .isZero();
    }

    @ParameterizedTest
    @MethodSource("invalidCreateBodies")
    void rejectsInvalidAndMalformedCreationBodies(String body) throws Exception {
        assertProblem(request("POST", "", body), 400);
    }

    static Stream<String> invalidCreateBodies() {
        return Stream.of("{", "null", "[]", "{}", "{\"sku\":null,\"onHand\":1}",
                "{\"sku\":\"\",\"onHand\":1}", "{\"sku\":\" A\",\"onHand\":1}",
                "{\"sku\":\"é\",\"onHand\":1}", "{\"sku\":\"-A\",\"onHand\":1}",
                "{\"sku\":\"" + "A".repeat(65) + "\",\"onHand\":1}",
                "{\"sku\":\"A\",\"onHand\":null}", "{\"sku\":\"A\",\"onHand\":-1}",
                "{\"sku\":\"A\",\"onHand\":1.5}", "{\"sku\":\"A\",\"onHand\":\"1\"}",
                "{\"sku\":\"A\",\"onHand\":true}",
                "{\"sku\":\"A\",\"onHand\":2147483648}", "{\"sku\":42,\"onHand\":1}",
                "{\"sku\":1.5,\"onHand\":1}", "{\"sku\":true,\"onHand\":1}",
                "{\"sku\":\"A\",\"onHand\":1,\"reserved\":1}");
    }

    @ParameterizedTest
    @MethodSource("invalidUpdateBodies")
    void rejectsInvalidUpdateBodiesWithoutChangingStock(String body) throws Exception {
        var sku = sku();
        create(sku, 4);
        assertProblem(request("PUT", "/" + sku, body), 400);
        assertStock(json(request("GET", "/" + sku, null)), sku, 4, 0);
    }

    static Stream<String> invalidUpdateBodies() {
        return Stream.of("{", "null", "{}", "{\"onHand\":null}", "{\"onHand\":-1}",
                "{\"onHand\":1.5}", "{\"onHand\":\"1\"}", "{\"onHand\":2147483648}",
                "{\"onHand\":false}",
                "{\"onHand\":3,\"reserved\":1}");
    }

    @Test
    void validatesPathSku() throws Exception {
        assertProblem(request("GET", "/-invalid", null), 400);
        assertProblem(request("PUT", "/-invalid", "{\"onHand\":1}"), 400);
    }

    @Test
    void acceptsZeroMaximumIntegerAndMaximumLengthSku() throws Exception {
        var sku = "Z" + UUID.randomUUID().toString().replace("-", "") + "a".repeat(31);
        assertThat(sku).hasSize(64);
        create(sku, 0);
        assertStock(json(request("GET", "/" + sku, null)), sku, 0, 0);
        var updated = request("PUT", "/" + sku, "{\"onHand\":2147483647}");
        assertThat(updated.statusCode()).isEqualTo(200);
        assertStock(json(updated), sku, Integer.MAX_VALUE, 0);
    }

    @Test
    void loweringBelowReservedConflictsAndLeavesStateUnchanged() throws Exception {
        var sku = sku();
        create(sku, 10);
        jdbc.update("UPDATE stock_items SET reserved = 7 WHERE sku = ?", sku);
        assertProblem(request("PUT", "/" + sku, "{\"onHand\":6}"), 409);
        assertStock(json(request("GET", "/" + sku, null)), sku, 10, 7);
        var equal = request("PUT", "/" + sku, "{\"onHand\":7}");
        assertThat(equal.statusCode()).isEqualTo(200);
        assertStock(json(equal), sku, 7, 7);
    }

    @Test
    void concurrentAdjustmentWaitsForLockAndChecksLatestReservedState() throws Exception {
        var sku = sku();
        create(sku, 10);
        try (Connection connection = dataSource.getConnection(); var executor = Executors.newSingleThreadExecutor()) {
            connection.setAutoCommit(false);
            try (var statement = connection.prepareStatement("UPDATE stock_items SET reserved = 7 WHERE sku = ?")) {
                statement.setString(1, sku);
                statement.executeUpdate();
            }
            var adjustment = executor.submit(() -> request("PUT", "/" + sku, "{\"onHand\":6}"));
            try {
                var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                boolean blocked = false;
                while (System.nanoTime() < deadline) {
                    blocked = Boolean.TRUE.equals(jdbc.queryForObject("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND wait_event_type = 'Lock' AND query LIKE '%stock_items%')", Boolean.class));
                    if (blocked) {
                        break;
                    }
                    Thread.sleep(25);
                }
                assertThat(blocked).as("HTTP stock update waits for PostgreSQL row lock").isTrue();
                assertThat(adjustment.isDone()).isFalse();
                connection.commit();
                assertProblem(adjustment.get(10, TimeUnit.SECONDS), 409);
            } finally {
                connection.rollback();
            }
        }
        assertStock(json(request("GET", "/" + sku, null)), sku, 10, 7);
    }

    @ParameterizedTest
    @MethodSource("invalidDatabaseValues")
    void databaseRejectsInvalidState(Integer onHand, Integer reserved) {
        assertThatThrownBy(() -> jdbc.update("INSERT INTO stock_items (id, sku, on_hand, reserved) VALUES (?, ?, ?, ?)",
                UUID.randomUUID(), sku(), onHand, reserved)).isInstanceOf(DataIntegrityViolationException.class)
                .rootCause().isInstanceOf(SQLException.class)
                .extracting(cause -> ((SQLException) cause).getSQLState())
                .isEqualTo(onHand == null || reserved == null ? "23502" : "23514");
    }

    static Stream<Arguments> invalidDatabaseValues() {
        return Stream.of(Arguments.of(-1, 0), Arguments.of(1, -1), Arguments.of(1, 2),
                Arguments.of(null, 0), Arguments.of(1, null));
    }

    @Test
    void databaseEnforcesUniqueSku() {
        var sku = sku();
        jdbc.update("INSERT INTO stock_items (id, sku, on_hand, reserved) VALUES (?, ?, 0, 0)", UUID.randomUUID(), sku);
        assertThatThrownBy(() -> jdbc.update("INSERT INTO stock_items (id, sku, on_hand, reserved) VALUES (?, ?, 0, 0)",
                UUID.randomUUID(), sku)).isInstanceOf(DataIntegrityViolationException.class)
                .rootCause().isInstanceOf(SQLException.class)
                .extracting(cause -> ((SQLException) cause).getSQLState()).isEqualTo("23505");
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" invalid", "SKU ", "é", "A/B", "A\n"})
    void databaseRejectsInvalidSku(String sku) {
        assertThatThrownBy(() -> jdbc.update("INSERT INTO stock_items (id, sku, on_hand, reserved) VALUES (?, ?, 0, 0)",
                UUID.randomUUID(), sku)).isInstanceOf(DataIntegrityViolationException.class)
                .rootCause().isInstanceOf(SQLException.class)
                .extracting(cause -> ((SQLException) cause).getSQLState()).isEqualTo(sku == null ? "23502" : "23514");
    }

    private String sku() {
        return "SKU-" + UUID.randomUUID();
    }

    private void create(String sku, int onHand) throws Exception {
        assertThat(request("POST", "", "{\"sku\":\"" + sku + "\",\"onHand\":" + onHand + "}").statusCode())
                .isEqualTo(201);
    }

    private HttpResponse<String> request(String method, String suffix, String body) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/stock-items" + suffix))
                .timeout(Duration.ofSeconds(10))
                .header("Authorization", "Bearer " + io.github.panteliszara.warexis.inventory.security.TestJwtConfiguration.operatorToken());
        builder.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        if (body != null) {
            builder.header("Content-Type", "application/json");
        }
        try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
            return client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        }
    }

    private JsonNode json(HttpResponse<String> response) {
        return mapper.readTree(response.body());
    }

    private void assertStock(JsonNode stock, String sku, int onHand, int reserved) {
        assertThat(stock.size()).isEqualTo(5);
        assertThat(stock.path("sku").asString()).isEqualTo(sku);
        assertThat(stock.path("onHand").asInt()).isEqualTo(onHand);
        assertThat(stock.path("reserved").asInt()).isEqualTo(reserved);
        assertThat(stock.path("available").asInt()).isEqualTo(onHand - reserved);
    }

    private void assertProblem(HttpResponse<String> response, int status) {
        assertThat(response.statusCode()).isEqualTo(status);
        assertThat(response.headers().firstValue("Content-Type")).hasValueSatisfying(value ->
                assertThat(value).startsWith("application/problem+json"));
        var problem = json(response);
        assertThat(problem.path("status").asInt()).isEqualTo(status);
        assertThat(problem.path("detail").asString()).isNotBlank();
        assertThat(response.body()).doesNotContain("SQLException", "stackTrace", "org.hibernate", "INSERT INTO", "stock_items_");
    }
}
