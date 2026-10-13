package io.github.panteliszara.warexis.order;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import tools.jackson.databind.ObjectMapper;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(OrderServiceApplicationTests.PostgresConfiguration.class)
class OrderSecurityTests {
    private static final String ORDER = "/api/orders/00000000-0000-0000-0000-000000000001";
    private static final String BODY = "{\"lines\":[{\"sku\":\"SECURITY-TEST\",\"quantity\":1}]}";
    @LocalServerPort int port;
    @Autowired ObjectMapper mapper;

    @Test void healthIsAnonymousAndHidesDetails() throws Exception {
        var response = request("GET", "/actuator/health", null, null);
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(mapper.readTree(response.body()).path("status").asString()).isEqualTo("UP");
        assertThat(response.body()).doesNotContain("components", "details", "jdbc", "password");
        assertThat(response.headers().allValues("Set-Cookie")).isEmpty();
    }
    @Test void missingTokenGetsGenericBearerChallengeWithoutSession() throws Exception {
        var response = request("GET", ORDER, null, null);
        problem(response, 401, "Authentication is required.");
        assertThat(response.headers().firstValue("WWW-Authenticate")).contains("Bearer");
    }
    @ParameterizedTest
    @ValueSource(strings = {"malformed", "unsigned", "signature", "issuer", "audience", "missing-audience", "expired", "future-not-before", "missing-expiration"})
    void rejectsInvalidTokensWithoutLeakingDetails(String kind) throws Exception {
        String token = switch (kind) {
            case "malformed" -> "not-a-jwt";
            case "unsigned" -> new com.nimbusds.jwt.PlainJWT(new com.nimbusds.jwt.JWTClaimsSet.Builder()
                    .issuer(TestJwtTokens.ISSUER).subject("test").audience(TestJwtTokens.AUDIENCE)
                    .expirationTime(Date.from(Instant.now().plusSeconds(300))).build()).serialize();
            case "signature" -> TestJwtTokens.badSignature();
            case "issuer" -> TestJwtTokens.token("WAREXIS_OPERATOR", c -> c.issuer("https://other.example.test"));
            case "audience" -> TestJwtTokens.token("WAREXIS_OPERATOR", c -> c.audience("other-api"));
            case "missing-audience" -> TestJwtTokens.token("WAREXIS_OPERATOR", c -> c.audience(List.of()));
            case "expired" -> TestJwtTokens.token("WAREXIS_OPERATOR", c -> { c.issueTime(Date.from(Instant.now().minusSeconds(900))); c.notBeforeTime(Date.from(Instant.now().minusSeconds(900))); c.expirationTime(Date.from(Instant.now().minusSeconds(300))); });
            case "future-not-before" -> TestJwtTokens.token("WAREXIS_OPERATOR", c -> c.notBeforeTime(Date.from(Instant.now().plusSeconds(120))));
            case "missing-expiration" -> TestJwtTokens.token("WAREXIS_OPERATOR", c -> c.expirationTime(null));
            default -> throw new IllegalArgumentException("Unknown case");
        };
        var response = request("GET", ORDER, token, null);
        problem(response, 401, "Authentication is required.");
        assertThat(response.headers().firstValue("WWW-Authenticate")).contains("Bearer error=\"invalid_token\"");
        assertThat(response.body()).doesNotContain(token, "issuer", "signature", "expired", "Jwt");
    }
    @Test void duplicateBearerHeadersRetainInvalidRequestSemantics() throws Exception {
        var token = TestJwtTokens.operator();
        var builder = HttpRequest.newBuilder(uri(ORDER)).timeout(Duration.ofSeconds(10))
                .header("Authorization", "Bearer " + token)
                .header("Authorization", "Bearer " + token).GET();
        try (var client = HttpClient.newHttpClient()) {
            var response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            problem(response, 400, "The request is invalid.");
            assertThat(response.headers().firstValue("WWW-Authenticate"))
                    .contains("Bearer error=\"invalid_request\"");
            assertThat(mapper.readTree(response.body()).path("title").asString()).isEqualTo("Bad Request");
            assertThat(response.body()).doesNotContain(token, "multiple", "Authorization", "Jwt");
        }
    }
    @Test void viewerReadsButCannotCreate() throws Exception {
        assertThat(request("GET", ORDER, TestJwtTokens.viewer(), null).statusCode()).isEqualTo(404);
        var mutation = request("POST", "/api/orders", TestJwtTokens.viewer(), BODY);
        problem(mutation, 403, "Access is denied.");
        assertThat(mutation.headers().firstValue("WWW-Authenticate")).contains("Bearer error=\"insufficient_scope\"");
    }
    @Test void operatorCreatesAndReadsWithoutViewerRoleOrSession() throws Exception {
        var created = request("POST", "/api/orders", TestJwtTokens.operator(), BODY);
        assertThat(created.statusCode()).isEqualTo(201);
        var id = UUID.fromString(mapper.readTree(created.body()).path("id").asString());
        var retrieved = request("GET", "/api/orders/" + id, TestJwtTokens.operator(), null);
        assertThat(retrieved.statusCode()).isEqualTo(200);
        assertThat(mapper.readTree(retrieved.body())).isEqualTo(mapper.readTree(created.body()));
        assertThat(created.headers().allValues("Set-Cookie")).isEmpty();
        assertThat(retrieved.headers().allValues("Set-Cookie")).isEmpty();
    }
    @ParameterizedTest
    @ValueSource(strings = {"unknown-role", "lookalike", "scope", "client-role", "malformed-realm", "malformed-roles", "missing-realm"})
    void onlyExactRealmBusinessRolesGrantAuthorities(String kind) throws Exception {
        String token = switch (kind) {
            case "unknown-role" -> TestJwtTokens.token("ADMIN", c -> {});
            case "lookalike" -> TestJwtTokens.token("ROLE_WAREXIS_OPERATOR", c -> {});
            case "scope" -> TestJwtTokens.token("other", c -> c.claim("scope", "WAREXIS_VIEWER WAREXIS_OPERATOR"));
            case "client-role" -> TestJwtTokens.token("other", c -> c.claim("resource_access", Map.of("warexis-api", Map.of("roles", List.of("WAREXIS_OPERATOR")))));
            case "malformed-realm" -> TestJwtTokens.token("other", c -> c.claim("realm_access", "WAREXIS_OPERATOR"));
            case "malformed-roles" -> TestJwtTokens.token("other", c -> c.claim("realm_access", Map.of("roles", "WAREXIS_OPERATOR")));
            case "missing-realm" -> TestJwtTokens.token("other", c -> c.claim("realm_access", null));
            default -> throw new IllegalArgumentException("Unknown case");
        };
        problem(request("GET", ORDER, token, null), 403, "Access is denied.");
        problem(request("POST", "/api/orders", token, BODY), 403, "Access is denied.");
    }
    @ParameterizedTest
    @ValueSource(strings = {"/actuator", "/actuator/env", "/actuator/health/readiness", "/login", "/logout", "/error", "/unknown", "/api/orders", "/.well-known/oauth-protected-resource"})
    void unclassifiedRoutesAreDeniedEvenForOperator(String route) throws Exception {
        problem(request("GET", route, TestJwtTokens.operator(), null), 403, "Access is denied.");
        problem(request("GET", route, null, null), 401, "Authentication is required.");
    }
    @ParameterizedTest @ValueSource(strings = {"HEAD", "OPTIONS", "PUT", "DELETE"})
    void unclassifiedMethodsAreDenied(String method) throws Exception {
        var response = request(method, "/actuator/health", TestJwtTokens.operator(), null);
        assertThat(response.statusCode()).isEqualTo(403);
        assertThat(response.headers().allValues("Set-Cookie")).isEmpty();
    }
    @Test void basicCredentialsAndSessionCookiesDoNotAuthenticate() throws Exception {
        var builder = HttpRequest.newBuilder(uri(ORDER)).header("Authorization", "Basic dGVzdDp0ZXN0").header("Cookie", "JSESSIONID=test-session").GET();
        try (var client = HttpClient.newHttpClient()) {
            var response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            problem(response, 401, "Authentication is required.");
            assertThat(response.headers().firstValue("WWW-Authenticate")).contains("Bearer");
        }
    }
    private HttpResponse<String> request(String method, String route, String token, String body) throws Exception {
        var builder = HttpRequest.newBuilder(uri(route)).timeout(Duration.ofSeconds(10));
        if (token != null) builder.header("Authorization", "Bearer " + token);
        if (body != null) builder.header("Content-Type", "application/json");
        builder.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        try (var client = HttpClient.newHttpClient()) { return client.send(builder.build(), HttpResponse.BodyHandlers.ofString()); }
    }
    private URI uri(String route) { return URI.create("http://127.0.0.1:" + port + route); }
    private void problem(HttpResponse<String> response, int status, String detail) throws Exception {
        assertThat(response.statusCode()).isEqualTo(status);
        assertThat(response.headers().firstValue("Content-Type").orElseThrow()).startsWith("application/problem+json");
        assertThat(mapper.readTree(response.body()).path("status").asInt()).isEqualTo(status);
        assertThat(mapper.readTree(response.body()).path("detail").asString()).isEqualTo(detail);
        assertThat(response.headers().allValues("Set-Cookie")).isEmpty();
        assertThat(response.headers().allValues("Location")).isEmpty();
    }
}
