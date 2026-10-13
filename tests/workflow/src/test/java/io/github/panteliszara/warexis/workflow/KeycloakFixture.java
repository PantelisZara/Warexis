package io.github.panteliszara.warexis.workflow;

import java.net.ServerSocket;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Test-owned identity server. Secrets and access tokens stay in memory, never in diagnostics. */
final class KeycloakFixture implements AutoCloseable {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final Network network = Network.newNetwork();
    private final PostgreSQLContainer database = new PostgreSQLContainer("postgres:18.6")
            .withNetwork(network).withNetworkAliases("identity-db").withDatabaseName("identity")
            .withUsername("identity").withPassword(UUID.randomUUID().toString());
    private GenericContainer<?> server;
    private String base;
    private String adminSecret = UUID.randomUUID().toString();
    private final Map<String, String> secrets = new HashMap<>();
    private final Map<String, Token> tokens = new HashMap<>();

    void start() throws Exception {
        int port;
        try (var socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
        base = "http://127.0.0.1:" + port;
        database.start();
        server = new GenericContainer<>("quay.io/keycloak/keycloak:26.8.0")
                .withNetwork(network).withExposedPorts(8080)
                .withEnv("KC_DB", "postgres")
                .withEnv("KC_DB_URL", "jdbc:postgresql://identity-db:5432/identity")
                .withEnv("KC_DB_USERNAME", database.getUsername()).withEnv("KC_DB_PASSWORD", database.getPassword())
                .withEnv("KC_HOSTNAME", base).withEnv("KC_HEALTH_ENABLED", "true").withEnv("KC_METRICS_ENABLED", "true")
                .withEnv("KC_BOOTSTRAP_ADMIN_CLIENT_ID", "workflow-bootstrap")
                .withEnv("KC_BOOTSTRAP_ADMIN_CLIENT_SECRET", adminSecret)
                .withCopyFileToContainer(MountableFile.forHostPath(Path.of(System.getProperty("keycloak.realm",
                        "../../infrastructure/auth/warexis-realm.json")).toAbsolutePath()), "/opt/keycloak/data/import/warexis-realm.json")
                .withCommand("start-dev", "--import-realm")
                .waitingFor(Wait.forHttp("/realms/warexis/.well-known/openid-configuration").forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(3)));
        server.setPortBindings(List.of("127.0.0.1:" + port + ":8080"));
        server.start();
        var discovery = json("GET", base + "/realms/warexis/.well-known/openid-configuration", null, null, 200);
        require(discovery.path("issuer").asString().equals(issuer()), "OIDC issuer");
        var jwks = json("GET", discovery.path("jwks_uri").asString(), null, null, 200);
        require(jwks.path("keys").isArray() && !jwks.path("keys").isEmpty(), "JWKS keys");
        var admin = grant("master", "workflow-bootstrap", adminSecret).value();
        provision(admin, "operator", "WAREXIS_OPERATOR", true, false, List.of());
        provision(admin, "viewer", "WAREXIS_VIEWER", true, false, List.of());
        provision(admin, "wrong-audience", "WAREXIS_OPERATOR", false, false, List.of(Map.of(
                "name", "other-audience", "protocol", "openid-connect", "protocolMapper", "oidc-audience-mapper",
                "config", Map.of("included.custom.audience", "unrelated-api", "access.token.claim", "true", "id.token.claim", "false"))));
        provision(admin, "missing-audience", "WAREXIS_OPERATOR", false, false, List.of());
        provision(admin, "expired", "WAREXIS_OPERATOR", true, true, List.of());
        json("POST", base + "/admin/realms/warexis/client-scopes", Map.of("name", "WAREXIS_OPERATOR", "protocol", "openid-connect",
                "attributes", Map.of("include.in.token.scope", "true")), admin, 201);
        provision(admin, "scope-only", null, true, false, List.of());
        provision(admin, "client-role-only", null, true, false, List.of(mapper("resource_access.warexis-api.roles", "WAREXIS_OPERATOR")));
        provision(admin, "lookalike", null, true, false, List.of(mapper("realm_access.roles", "WAREXIS_OPERATOR_EXTRA")));
        // A distinct actual issuer issues its own signed access tokens, without changing the API realm.
        json("POST", base + "/admin/realms", Map.of("realm", "foreign", "enabled", true, "sslRequired", "none"), admin, 201);
        var foreignSecret = UUID.randomUUID().toString();
        secrets.put("foreign", foreignSecret);
        json("POST", base + "/admin/realms/foreign/clients", client("foreign", foreignSecret, List.of(), false, List.of(Map.of(
                "name", "foreign-api-audience", "protocol", "openid-connect", "protocolMapper", "oidc-audience-mapper",
                "config", Map.of("included.custom.audience", "warexis-api", "access.token.claim", "true", "id.token.claim", "false")))), admin, 201);
        var operator = claims(token("operator"));
        require(operator.path("iss").asString().equals(issuer()), "signed access token issuer");
        require(hasAudience(operator, "warexis-api"), "signed API audience");
        require(hasRole(operator, "WAREXIS_OPERATOR") && hasRole(operator, "WAREXIS_VIEWER"), "operator composite realm roles");
        require(hasRole(claims(token("viewer")), "WAREXIS_VIEWER") && !hasRole(claims(token("viewer")), "WAREXIS_OPERATOR"), "viewer realm role");
        require(hasAudience(claims(token("wrong-audience")), "unrelated-api")
                && !hasAudience(claims(token("wrong-audience")), "warexis-api"), "explicit wrong audience fixture");
        require(!hasAudience(claims(token("missing-audience")), "warexis-api"), "missing audience fixture");
        require(hasAudience(claims(token("foreign")), "warexis-api")
                && !claims(token("foreign")).path("iss").asString().equals(issuer()), "wrong issuer with correct API audience");
        require(claims(token("scope-only")).path("scope").asString().contains("WAREXIS_OPERATOR"), "scope-only negative claim");
        require(arrayContains(claims(token("client-role-only")).path("resource_access").path("warexis-api").path("roles"),
                "WAREXIS_OPERATOR"), "client-role-only negative claim array");
        require(arrayContains(claims(token("lookalike")).path("realm_access").path("roles"),
                "WAREXIS_OPERATOR_EXTRA"), "lookalike negative realm-role array");
        // Bootstrap is no longer necessary after the disposable clients are provisioned.
        var bootstrap = json("GET", base + "/admin/realms/master/clients?clientId=workflow-bootstrap", null, admin, 200);
        json("DELETE", base + "/admin/realms/master/clients/" + bootstrap.get(0).path("id").asString(), null, admin, 204);
        adminSecret = null;
    }

    String issuer() { return base + "/realms/warexis"; }

    synchronized String token(String client) {
        var cached = tokens.get(client);
        if (cached == null || Instant.now().plusSeconds(30).isAfter(cached.expires())) {
            cached = grant(client.equals("foreign") ? "foreign" : "warexis", client, secrets.get(client));
            tokens.put(client, cached);
        }
        return cached.value();
    }

    private void provision(String admin, String name, String role, boolean audience, boolean shortLived,
                           List<Map<String, Object>> mappers) throws Exception {
        var secret = UUID.randomUUID().toString();
        secrets.put(name, secret);
        json("POST", base + "/admin/realms/warexis/clients", client(name, secret,
                name.equals("scope-only") ? List.of("roles", "warexis-api", "WAREXIS_OPERATOR")
                        : audience ? List.of("roles", "warexis-api") : List.of("roles"), shortLived, mappers), admin, 201);
        var clients = json("GET", base + "/admin/realms/warexis/clients?clientId=" + name, null, admin, 200);
        var id = clients.get(0).path("id").asString();
        if (role != null) {
            var representation = JSON.readValue(json("GET", base + "/admin/realms/warexis/roles/" + role, null, admin, 200).toString(), Map.class);
            var account = json("GET", base + "/admin/realms/warexis/clients/" + id + "/service-account-user", null, admin, 200);
            json("POST", base + "/admin/realms/warexis/users/" + account.path("id").asString() + "/role-mappings/realm", List.of(representation), admin, 204);
            json("POST", base + "/admin/realms/warexis/clients/" + id + "/scope-mappings/realm", List.of(representation), admin, 204);
        }
    }

    private static Map<String, Object> client(String name, String secret, List<String> scopes, boolean shortLived,
                                               List<Map<String, Object>> mappers) {
        return Map.ofEntries(Map.entry("clientId", name), Map.entry("protocol", "openid-connect"),
                Map.entry("publicClient", false), Map.entry("secret", secret), Map.entry("serviceAccountsEnabled", true),
                Map.entry("standardFlowEnabled", false), Map.entry("implicitFlowEnabled", false),
                Map.entry("directAccessGrantsEnabled", false), Map.entry("fullScopeAllowed", false),
                Map.entry("defaultClientScopes", scopes), Map.entry("optionalClientScopes", List.of()),
                Map.entry("protocolMappers", mappers), Map.entry("attributes", shortLived ? Map.of("access.token.lifespan", "1") : Map.of()));
    }

    private static Map<String, Object> mapper(String claim, String value) {
        return Map.of("name", "negative-" + claim, "protocol", "openid-connect", "protocolMapper", "oidc-hardcoded-claim-mapper",
                "config", Map.of("claim.name", claim, "claim.value", claim.equals("scope") ? value : JSON.writeValueAsString(List.of(value)), "jsonType.label", claim.equals("scope") ? "String" : "JSON", "access.token.claim", "true", "id.token.claim", "false"));
    }

    private Token grant(String realm, String client, String secret) {
        try {
            var body = "grant_type=client_credentials&client_id=" + encode(client) + "&client_secret=" + encode(secret);
            var response = HTTP.send(HttpRequest.newBuilder(URI.create(base + "/realms/" + realm + "/protocol/openid-connect/token"))
                    .timeout(Duration.ofSeconds(10)).header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
            require(response.statusCode() == 200, "client credentials token request");
            var result = JSON.readTree(response.body());
            require(result.path("access_token").isString(), "access token response");
            var token = result.path("access_token").asString();
            return new Token(token, Instant.ofEpochSecond(claims(token).path("exp").asLong()));
        } catch (Exception failure) { throw new IllegalStateException("Identity token request failed"); }
    }

    static JsonNode claims(String token) {
        // Test-only observation of a token issued by the actual server; never an authentication implementation.
        return JSON.readTree(Base64.getUrlDecoder().decode(token.split("\\.")[1]));
    }

    static boolean hasRole(JsonNode token, String role) {
        return arrayContains(token.path("realm_access").path("roles"), role);
    }
    private static boolean arrayContains(JsonNode claim, String value) {
        if (!claim.isArray()) return false;
        for (var item : claim) if (value.equals(item.asString())) return true;
        return false;
    }
    private static boolean hasAudience(JsonNode token, String audience) {
        var claim = token.path("aud");
        if (claim.isString()) return audience.equals(claim.asString());
        for (var item : claim) if (audience.equals(item.asString())) return true;
        return false;
    }
    private static String encode(String value) { return URLEncoder.encode(value, StandardCharsets.UTF_8); }
    private static JsonNode json(String method, String url, Object body, String bearer, int expected) throws Exception {
        var request = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10)).header("Content-Type", "application/json");
        if (bearer != null) request.header("Authorization", "Bearer " + bearer);
        var response = HTTP.send(request.method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body))).build(), HttpResponse.BodyHandlers.ofString());
        require(response.statusCode() == expected, "Identity management operation " + method + " expected " + expected + " got " + response.statusCode());
        return response.body().isBlank() ? JSON.nullNode() : JSON.readTree(response.body());
    }
    private static void require(boolean condition, String label) { if (!condition) throw new AssertionError(label); }
    @Override public void close() {
        try { if (server != null) server.stop(); }
        finally { try { database.stop(); } finally { network.close(); secrets.clear(); tokens.clear(); } }
    }
    private record Token(String value, Instant expires) { }
}
