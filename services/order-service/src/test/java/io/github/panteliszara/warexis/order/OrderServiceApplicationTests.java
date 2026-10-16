package io.github.panteliszara.warexis.order;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.sql.SQLException;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import jakarta.persistence.EntityManagerFactory;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.ObjectMapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.security.oauth2.jwt.JwtDecoder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(OrderServiceApplicationTests.PostgresConfiguration.class)
class OrderServiceApplicationTests {

    @Autowired
    private DataSource dataSource;

    @Autowired
    private Flyway flyway;

    @Autowired
    private EntityManagerFactory entityManagerFactory;

    @Autowired
    private ObjectMapper objectMapper;

    @LocalServerPort
    private int port;

    @Test
    void contextStartsWithPostgresJpaAndFlyway() throws Exception {
        assertThat(entityManagerFactory.isOpen()).isTrue();
        assertThat(flyway.validateWithResult().validationSuccessful).isTrue();

        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            assertThat(connection.getMetaData().getDatabaseProductName()).isEqualTo("PostgreSQL");

            try (var result = statement.executeQuery("SELECT version, success FROM flyway_schema_history ORDER BY installed_rank")) {
                assertThat(result.next()).isTrue();
                assertThat(result.getString("version")).isEqualTo("1");
                assertThat(result.getBoolean("success")).isTrue();
                assertThat(result.next()).isTrue();
                assertThat(result.getString("version")).isEqualTo("2");
                assertThat(result.getBoolean("success")).isTrue();
                assertThat(result.next()).isTrue();
                assertThat(result.getString("version")).isEqualTo("3");
                assertThat(result.getBoolean("success")).isTrue();
                assertThat(result.next()).isTrue();
                assertThat(result.getString("version")).isEqualTo("4");
                assertThat(result.getBoolean("success")).isTrue();
                assertThat(result.next()).isFalse();
            }
        }
    }

    @Test
    void healthEndpointReportsUp() throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/actuator/health"))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();

        try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());

            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(objectMapper.readTree(response.body()).path("status").asString()).isEqualTo("UP");
        }
    }

    @Test
    void createsAndReloadsOrderWithStableLinesAndTimestamps() throws Exception {
        var response = post("""
                {"lines":[{"sku":"Abc-1", "quantity":2},{"sku":"Abc-1", "quantity":3},{"sku":"abc-1", "quantity":1}]}
                """);
        assertThat(response.statusCode()).isEqualTo(201);
        var created = objectMapper.readTree(response.body());
        var id = UUID.fromString(created.path("id").asString());
        assertThat(response.headers().firstValue("Location")).contains("/api/orders/" + id);
        assertThat(created.path("status").asString()).isEqualTo("PENDING_INVENTORY");
        assertThat(created.path("createdAt").asString()).isEqualTo(created.path("updatedAt").asString());
        assertThat(Instant.parse(created.path("createdAt").asString())).isBeforeOrEqualTo(Instant.now());
        assertThat(created.path("lines").size()).isEqualTo(3);
        assertThat(created.path("lines").get(0).path("sku").asString()).isEqualTo("Abc-1");
        assertThat(created.path("lines").get(1).path("quantity").asInt()).isEqualTo(3);
        assertThat(created.path("lines").get(2).path("sku").asString()).isEqualTo("abc-1");
        var firstLineId = UUID.fromString(created.path("lines").get(0).path("id").asString());
        assertThat(firstLineId).isNotEqualTo(UUID.fromString(created.path("lines").get(1).path("id").asString()));

        var retrieved = get("/api/orders/" + id);
        assertThat(retrieved.statusCode()).isEqualTo(200);
        assertThat(objectMapper.readTree(retrieved.body())).isEqualTo(created);
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement("SELECT id, sku, quantity FROM order_lines WHERE order_id = ? ORDER BY line_position")) {
            statement.setObject(1, id);
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getObject("id", UUID.class)).isEqualTo(firstLineId);
                assertThat(rows.getString("sku")).isEqualTo("Abc-1");
                assertThat(rows.getInt("quantity")).isEqualTo(2);
                assertThat(rows.next()).isTrue();
                assertThat(rows.getInt("quantity")).isEqualTo(3);
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString("sku")).isEqualTo("abc-1");
                assertThat(rows.next()).isFalse();
            }
        }
    }

    @Test
    void acceptsMaximumLinesSkuLengthAndIntegerQuantity() throws Exception {
        var line = "{\"sku\":\"" + "A".repeat(64) + "\",\"quantity\":2147483647}";
        var lines = IntStream.range(0, 100).mapToObj(i -> line).collect(Collectors.joining(","));
        var response = post("{\"lines\":[" + lines + "]}");
        assertThat(response.statusCode()).isEqualTo(201);
        var created = objectMapper.readTree(response.body());
        assertThat(created.path("lines").size()).isEqualTo(100);
        var retrieved = get("/api/orders/" + created.path("id").asString());
        assertThat(retrieved.statusCode()).isEqualTo(200);
        assertThat(objectMapper.readTree(retrieved.body())).isEqualTo(created);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{}", "null", "{\"lines\":null}", "{\"lines\":[]}", "{\"lines\":[null]}",
            "{\"lines\":[{\"sku\":42,\"quantity\":1}]}",
            "{\"lines\":[{\"sku\":1.5,\"quantity\":1}]}",
            "{\"lines\":[{\"sku\":true,\"quantity\":1}]}",
            "{\"lines\":[{}]}", "{\"lines\":[{\"sku\":null,\"quantity\":1}]}",
            "{\"lines\":[{\"sku\":\"A\",\"quantity\":null}]}",
            "{\"lines\":[{\"sku\":\"A\",\"quantity\":0}]}",
            "{\"lines\":[{\"sku\":\"A\",\"quantity\":-1}]}",
            "{\"lines\":[{\"sku\":\"A\",\"quantity\":1.5}]}",
            "{\"lines\":[{\"sku\":\"A\",\"quantity\":1.0}]}",
            "{\"lines\":[{\"sku\":\"A\",\"quantity\":\"2\"}]}",
            "{\"lines\":[{\"sku\":\"A\",\"quantity\":true}]}",
            "{\"lines\":[{\"sku\":\"A\",\"quantity\":2147483648}]}",
            "{\"lines\":[{\"sku\":\" a\",\"quantity\":1}]}",
            "{\"lines\":[{\"sku\":\"a \",\"quantity\":1}]}",
            "{\"lines\":[{\"sku\":\"é\",\"quantity\":1}]}",
            "{\"lines\":[{\"sku\":\"a/b\",\"quantity\":1}]}",
            "{\"lines\":[{\"sku\":\"\",\"quantity\":1}]}",
            "{\"lines\":[{\"sku\":\"-a\",\"quantity\":1}]}",
            "{\"lines\":[{\"sku\":\"A\",\"quantity\":1},{\"sku\":\"B\",\"quantity\":0}]}",
            "{", "{\"lines\":\"invalid\"}"
    })
    void rejectsInvalidRequestsWithoutPartialPersistence(String body) throws Exception {
        var ordersBefore = countRows("orders");
        var linesBefore = countRows("order_lines");
        var eventsBefore = countRows("outbox_events");
        assertProblem(post(body), 400);
        assertThat(countRows("orders")).isEqualTo(ordersBefore);
        assertThat(countRows("order_lines")).isEqualTo(linesBefore);
        assertThat(countRows("outbox_events")).isEqualTo(eventsBefore);
    }

    @Test
    void rejectsTooManyLinesAndTooLongSku() throws Exception {
        var countBefore = countRows("orders");
        var lines = IntStream.range(0, 101).mapToObj(i -> "{\"sku\":\"A\",\"quantity\":1}")
                .collect(Collectors.joining(","));
        assertProblem(post("{\"lines\":[" + lines + "]}"), 400);
        assertProblem(post("{\"lines\":[{\"sku\":\"" + "A".repeat(65) + "\",\"quantity\":1}]}"), 400);
        assertThat(countRows("orders")).isEqualTo(countBefore);
    }

    @Test
    void returnsSafeProblemsForMissingOrderAndMalformedId() throws Exception {
        assertProblem(get("/api/orders/" + UUID.randomUUID()), 404);
        assertProblem(get("/api/orders/not-a-uuid"), 400);
        assertProblem(get("/api/orders/1-1-1-1-1"), 400);
    }

    @Test
    void preservesUnsupportedMethodAndMediaTypeSemantics() throws Exception {
        try (var client = HttpClient.newHttpClient()) {
            var uri = URI.create("http://localhost:" + port + "/api/orders");
            var put = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(10))
                    .header("Authorization", "Bearer " + TestJwtTokens.operator())
                    .header("Content-Type", "application/json").PUT(HttpRequest.BodyPublishers.ofString("{}"))
                    .build();
            var putResponse = client.send(put, HttpResponse.BodyHandlers.ofString());
            // Undefined methods are deliberately denied before reaching MVC.
            assertProblem(putResponse, 403);
            var unsupported = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(10))
                    .header("Authorization", "Bearer " + TestJwtTokens.operator())
                    .header("Content-Type", "text/plain").POST(HttpRequest.BodyPublishers.ofString("{}"))
                    .build();
            assertProblem(client.send(unsupported, HttpResponse.BodyHandlers.ofString()), 415);
        }
    }

    @Test
    void postgresEnforcesOrderAndLineConstraints() throws Exception {
        var orderId = UUID.randomUUID();
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("INSERT INTO orders VALUES ('" + orderId + "', 'PENDING_INVENTORY', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)");
            assertSqlState(statement, "INSERT INTO orders VALUES (gen_random_uuid(), 'INVALID', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)", "23514");
            assertSqlState(statement, "INSERT INTO orders VALUES (gen_random_uuid(), NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)", "23502");
            assertSqlState(statement, "INSERT INTO orders VALUES (gen_random_uuid(), 'PENDING_INVENTORY', NULL, CURRENT_TIMESTAMP)", "23502");
            var prefix = "INSERT INTO order_lines VALUES (gen_random_uuid(), '" + orderId + "', ";
            assertSqlState(statement, prefix + "0, 'A', 0)", "23514");
            assertSqlState(statement, prefix + "0, 'A', -1)", "23514");
            assertSqlState(statement, prefix + "0, 'a/b', 1)", "23514");
            assertSqlState(statement, prefix + "0, 'é', 1)", "23514");
            assertSqlState(statement, prefix + "0, NULL, 1)", "23502");
            assertSqlState(statement, prefix + "0, 'A', NULL)", "23502");
            assertSqlState(statement, prefix + "100, 'A', 1)", "23514");
            assertSqlState(statement, "INSERT INTO order_lines VALUES (gen_random_uuid(), gen_random_uuid(), 0, 'A', 1)", "23503");
            statement.executeUpdate(prefix + "0, 'A', 1)");
            assertSqlState(statement, prefix + "0, 'B', 1)", "23505");
            try (var rows = statement.executeQuery("SELECT data_type FROM information_schema.columns WHERE table_name = 'orders' AND column_name IN ('created_at', 'updated_at')")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString(1)).isEqualTo("timestamp with time zone");
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString(1)).isEqualTo("timestamp with time zone");
                assertThat(rows.next()).isFalse();
            }
        }
    }

    private void assertSqlState(java.sql.Statement statement, String sql, String state) {
        assertThatExceptionOfType(SQLException.class).isThrownBy(() -> statement.executeUpdate(sql))
                .satisfies(exception -> assertThat(exception.getSQLState()).isEqualTo(state));
    }

    private long countRows(String table) throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var result = statement.executeQuery("SELECT COUNT(*) FROM " + table)) {
            assertThat(result.next()).isTrue();
            return result.getLong(1);
        }
    }

    private HttpResponse<String> post(String body) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/orders"))
                .timeout(Duration.ofSeconds(10)).header("Authorization", "Bearer " + TestJwtTokens.operator())
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build();
        try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            return client.send(request, HttpResponse.BodyHandlers.ofString());
        }
    }

    private HttpResponse<String> get(String path) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(10)).header("Authorization", "Bearer " + TestJwtTokens.operator()).GET().build();
        try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            return client.send(request, HttpResponse.BodyHandlers.ofString());
        }
    }

    private void assertProblem(HttpResponse<String> response, int expectedStatus) throws Exception {
        assertThat(response.statusCode()).isEqualTo(expectedStatus);
        assertThat(response.headers().firstValue("Content-Type").orElseThrow()).startsWith("application/problem+json");
        var problem = objectMapper.readTree(response.body());
        assertThat(problem.path("status").asInt()).isEqualTo(expectedStatus);
        assertThat(problem.path("detail").asString()).isNotBlank();
        assertThat(response.body()).doesNotContain("SQLException", "stackTrace", "org.hibernate", "SELECT ", "INSERT ");
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class PostgresConfiguration {

        @Bean
        @Primary
        JwtDecoder testJwtDecoder() throws com.nimbusds.jose.JOSEException {
            return TestJwtTokens.decoder();
        }

        @Bean
        @ServiceConnection
        PostgreSQLContainer postgres() {
            return new PostgreSQLContainer("postgres:18.6");
        }
    }
}
