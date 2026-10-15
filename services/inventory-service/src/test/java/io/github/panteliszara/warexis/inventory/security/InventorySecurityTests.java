package io.github.panteliszara.warexis.inventory.security;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {"WAREXIS_OAUTH2_ISSUER_URI=https://issuer.test/realms/warexis", "WAREXIS_OAUTH2_AUDIENCE=warexis-api"})
@Import(TestJwtConfiguration.class)
@Testcontainers
class InventorySecurityTests {
    @Container @ServiceConnection
    static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18.6");
    @LocalServerPort int port;

    @Test
    void exactHealthIsAnonymousAndDoesNotCreateSession() throws Exception {
        var response = request("GET", "/actuator/health", null, null);
        assertThat(response.statusCode()).isEqualTo(200);
        var health = tools.jackson.databind.json.JsonMapper.builder().build().readTree(response.body());
        assertThat(health.path("status").asString()).isEqualTo("UP");
        assertThat(health.has("components")).isFalse();
        assertThat(health.has("details")).isFalse();
        assertThat(response.headers().allValues("Set-Cookie")).isEmpty();
    }

    @Test
    void metricsRequireDedicatedRoleWhichCannotAccessBusinessRoutes() throws Exception {
        var metrics = "Bearer " + TestJwtConfiguration.token(claims -> claims.claim("realm_access", Map.of("roles", List.of("WAREXIS_METRICS"))));
        assertProblem(request("GET", "/actuator/prometheus", null, null), 401);
        assertProblem(request("GET", "/actuator/prometheus", "Bearer " + TestJwtConfiguration.operatorToken(), null), 403);
        var viewer = "Bearer " + TestJwtConfiguration.token(claims -> claims.claim("realm_access", Map.of("roles", List.of("WAREXIS_VIEWER"))));
        assertProblem(request("GET", "/actuator/prometheus", viewer, null), 403);
        var response = request("GET", "/actuator/prometheus", metrics, null);
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("warexis_messaging_deliveries_total", "service=\"inventory\"",
                "http_server_requests_seconds_bucket", "jvm_memory_used_bytes", "process_cpu_usage", "hikaricp_connections");
        assertThat(response.body()).doesNotContain("disk_free_bytes", "disk_total_bytes", "path=\"",
                System.getProperty("user.dir"), System.getProperty("user.home"));
        assertProblem(request("GET", "/api/stock-items/MISSING", metrics, null), 403);
        assertProblem(request("POST", "/api/stock-items", metrics, "{}"), 403);
        assertProblem(request("GET", "/actuator/env", metrics, null), 403);
    }

    @Test
    void missingAndBasicCredentialsAreUnauthenticated() throws Exception {
        assertProblem(request("GET", "/api/stock-items/MISSING", null, null), 401);
        assertProblem(request("GET", "/api/stock-items/MISSING", "Basic dGVzdDp0ZXN0", null), 401);
    }

    @Test
    void multipleBearerAuthorizationHeadersAreAnInvalidRequest() throws Exception {
        var token = TestJwtConfiguration.operatorToken();
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/stock-items/MISSING"))
                .timeout(java.time.Duration.ofSeconds(10))
                .header("Authorization", "Bearer " + token)
                .header("Authorization", "Bearer " + token)
                .GET().build();
        try (var client = HttpClient.newHttpClient()) {
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertProblem(response, 400);
            assertThat(response.headers().firstValue("WWW-Authenticate")).hasValueSatisfying(value ->
                    assertThat(value).contains("Bearer", "invalid_request").doesNotContain(token, "resource_metadata"));
            assertThat(response.body()).doesNotContain(token, "Exception", "Authorization", "Found multiple");
        }
    }

    @Test
    void queryAndFormCredentialsCannotAuthenticate() throws Exception {
        var token = TestJwtConfiguration.operatorToken();
        assertProblem(request("GET", "/api/stock-items/MISSING?access_token=" + token, null, null), 401);
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/stock-items"))
                .timeout(java.time.Duration.ofSeconds(10))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString("access_token=" + token)).build();
        try (var client = HttpClient.newHttpClient()) {
            assertProblem(client.send(request, HttpResponse.BodyHandlers.ofString()), 401);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"malformed", "signature", "expired", "future", "issuer", "audience", "missingAudience", "missingExpiry", "unsigned"})
    void rejectsInvalidTokensWithGenericBearerChallenge(String kind) throws Exception {
        String token = switch (kind) {
            case "malformed" -> "not.a.jwt";
            case "signature" -> TestJwtConfiguration.token(claims -> {}, TestJwtConfiguration.key());
            case "expired" -> TestJwtConfiguration.token(claims -> claims.expirationTime(Date.from(Instant.now().minusSeconds(120))));
            case "future" -> TestJwtConfiguration.token(claims -> claims.notBeforeTime(Date.from(Instant.now().plusSeconds(120))));
            case "issuer" -> TestJwtConfiguration.token(claims -> claims.issuer("https://wrong.test"));
            case "audience" -> TestJwtConfiguration.token(claims -> claims.audience("wrong-api"));
            case "missingAudience" -> TestJwtConfiguration.token(claims -> claims.audience((List<String>) null));
            case "missingExpiry" -> TestJwtConfiguration.token(claims -> claims.expirationTime(null));
            case "unsigned" -> "eyJhbGciOiJub25lIn0.eyJzdWIiOiJ0ZXN0In0.";
            default -> throw new IllegalArgumentException();
        };
        var response = request("GET", "/api/stock-items/MISSING", "Bearer " + token, null);
        assertProblem(response, 401);
        assertThat(response.headers().firstValue("WWW-Authenticate")).hasValueSatisfying(value ->
                assertThat(value).contains("Bearer", "invalid_token").doesNotContain("expired", "issuer", "signature"));
        assertThat(response.body()).doesNotContain(token, "Jwt", "Exception", "wrong.test");
    }

    @Test
    void viewerReadsButCannotCreateOrUpdateAndOperatorCanDoBoth() throws Exception {
        var sku = "SEC-" + UUID.randomUUID();
        var operator = "Bearer " + TestJwtConfiguration.operatorToken();
        var viewer = "Bearer " + TestJwtConfiguration.token(claims -> claims.claim("realm_access", Map.of("roles", List.of("WAREXIS_VIEWER"))));
        assertThat(request("POST", "/api/stock-items", operator, "{\"sku\":\"" + sku + "\",\"onHand\":4}").statusCode()).isEqualTo(201);
        assertThat(request("GET", "/api/stock-items/" + sku, viewer, null).statusCode()).isEqualTo(200);
        assertThat(request("GET", "/api/stock-items/" + sku, operator, null).statusCode()).isEqualTo(200);
        assertProblem(request("POST", "/api/stock-items", viewer, "{}"), 403);
        assertProblem(request("PUT", "/api/stock-items/" + sku, viewer, "{\"onHand\":5}"), 403);
        assertThat(request("PUT", "/api/stock-items/" + sku, operator, "{\"onHand\":5}").statusCode()).isEqualTo(200);
        assertThat(request("GET", "/api/stock-items/ABSENT", viewer, null).statusCode()).isEqualTo(404);
    }

    @ParameterizedTest
    @ValueSource(strings = {"/actuator", "/actuator/env", "/actuator/health/readiness", "/error", "/login", "/logout", "/api/stock-items", "/unknown", "/.well-known/oauth-protected-resource/nested"})
    void undeclaredRoutesAreDenied(String path) throws Exception {
        assertProblem(request("GET", path, "Bearer " + TestJwtConfiguration.operatorToken(), null), 403);
        assertProblem(request("GET", path, null, null), 401);
    }

    @Test
    void resourceMetadataIsDeniedForAnonymousAndAuthenticatedRequests() throws Exception {
        var path = "/.well-known/oauth-protected-resource";
        var anonymous = request("GET", path, null, null);
        var authenticated = request("GET", path, "Bearer " + TestJwtConfiguration.operatorToken(), null);
        assertThat(List.of(anonymous.statusCode(), authenticated.statusCode())).containsExactly(401, 403);
        assertProblem(anonymous, 401);
        assertProblem(authenticated, 403);
    }

    @ParameterizedTest
    @ValueSource(strings = {"HEAD", "OPTIONS", "DELETE", "PATCH"})
    void undeclaredMethodsCannotUseReadPermission(String method) throws Exception {
        assertThat(request(method, "/api/stock-items/MISSING", "Bearer " + TestJwtConfiguration.operatorToken(), null).statusCode()).isEqualTo(403);
    }

    @Test
    void scopesClientRolesAndRoleLookalikesCannotGrantAuthority() throws Exception {
        var token = TestJwtConfiguration.token(claims -> claims.claim("scope", "WAREXIS_OPERATOR")
                .claim("resource_access", Map.of("warexis-api", Map.of("roles", List.of("WAREXIS_OPERATOR"))))
                .claim("realm_access", Map.of("roles", List.of("ROLE_WAREXIS_OPERATOR", "warexis_operator", "WAREXIS_OPERATOR "))));
        assertProblem(request("GET", "/api/stock-items/MISSING", "Bearer " + token, null), 403);
    }

    private HttpResponse<String> request(String method, String path, String auth, String body) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(java.time.Duration.ofSeconds(10))
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        if (auth != null) request.header("Authorization", auth);
        if (body != null) request.header("Content-Type", "application/json");
        try (var client = HttpClient.newHttpClient()) {
            return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
        }
    }

    private void assertProblem(HttpResponse<String> response, int status) {
        assertThat(response.statusCode()).isEqualTo(status);
        assertThat(response.headers().firstValue("Content-Type")).hasValueSatisfying(value ->
                assertThat(value).startsWith("application/problem+json"));
        assertThat(response.body()).contains("\"status\":" + status);
        assertThat(response.headers().allValues("Set-Cookie")).isEmpty();
        if (status == 401) assertThat(response.headers().firstValue("WWW-Authenticate")).hasValueSatisfying(v -> assertThat(v).startsWith("Bearer").doesNotContain("resource_metadata"));
        if (status == 403) assertThat(response.headers().firstValue("WWW-Authenticate")).hasValueSatisfying(v -> assertThat(v).contains("insufficient_scope"));
    }
}
