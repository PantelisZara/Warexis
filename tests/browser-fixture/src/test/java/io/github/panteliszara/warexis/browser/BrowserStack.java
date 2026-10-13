package io.github.panteliszara.warexis.browser;

import java.net.ServerSocket;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Test-owned stack only. No browser driver, production resources, or persisted access tokens. */
final class BrowserStack implements AutoCloseable {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final FixtureConfig config;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Network network = Network.newNetwork();
    private final List<AutoCloseable> resources = new ArrayList<>();
    private final List<Process> processes = new ArrayList<>();
    private String base;
    private final RuntimePublication runtimePublication;

    BrowserStack(FixtureConfig config) {
        this.config = config;
        this.runtimePublication = new RuntimePublication(config.runtime());
    }

    void start() throws Exception {
        privateDirectory(config.runtime().getParent());
        var identityDb = new PostgreSQLContainer("postgres:18.6").withNetwork(network)
                .withNetworkAliases("identity-db").withDatabaseName("identity").withUsername("identity")
                .withPassword(UUID.randomUUID().toString());
        resources.add(identityDb);
        loopback(identityDb);
        identityDb.start();
        int identityPort = freePort();
        base = "http://127.0.0.1:" + identityPort;
        String bootstrapSecret = UUID.randomUUID().toString();
        var identity = new GenericContainer<>("quay.io/keycloak/keycloak:26.8.0")
                .withNetwork(network).withExposedPorts(8080)
                .withEnv("KC_DB", "postgres").withEnv("KC_DB_URL", "jdbc:postgresql://identity-db:5432/identity")
                .withEnv("KC_DB_USERNAME", identityDb.getUsername()).withEnv("KC_DB_PASSWORD", identityDb.getPassword())
                .withEnv("KC_HOSTNAME", base).withEnv("KC_HEALTH_ENABLED", "true")
                .withEnv("KC_BOOTSTRAP_ADMIN_CLIENT_ID", "browser-bootstrap")
                .withEnv("KC_BOOTSTRAP_ADMIN_CLIENT_SECRET", bootstrapSecret)
                .withCopyFileToContainer(MountableFile.forHostPath(config.realm()), "/opt/keycloak/data/import/warexis-realm.json")
                .withCommand("start-dev", "--import-realm")
                .waitingFor(Wait.forHttp("/realms/warexis/.well-known/openid-configuration").forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(3)));
        identity.setPortBindings(List.of("127.0.0.1:" + identityPort + ":8080"));
        resources.add(identity);
        identity.start();
        var discovery = request("GET", base + "/realms/warexis/.well-known/openid-configuration", null, null, 200);
        if (!issuer().equals(discovery.path("issuer").asString())) throw new IllegalStateException("Unexpected identity issuer");
        String admin = bootstrap(bootstrapSecret);
        var users = Map.of("viewer", user(admin, "viewer", "WAREXIS_VIEWER"),
                "operator", user(admin, "operator", "WAREXIS_OPERATOR"));
        var bootstrap = request("GET", base + "/admin/realms/master/clients?clientId=browser-bootstrap", null, admin, 200);
        request("DELETE", base + "/admin/realms/master/clients/" + bootstrap.get(0).path("id").asString(), null, admin, 204);
        // No test seeding client or SPA password grant: business writes happen through the operator browser flow.
        var kafka = new KafkaContainer("apache/kafka:4.2.2");
        resources.add(kafka);
        loopback(kafka);
        kafka.start();
        var services = new LinkedHashMap<String, String>();
        for (String name : List.of("order", "inventory", "fulfillment")) {
            var database = new PostgreSQLContainer("postgres:18.6").withPassword(UUID.randomUUID().toString());
            resources.add(database);
            loopback(database);
            database.start();
            int port = freePort();
            Path log = config.runtime().getParent().resolve(name + ".log");
            privateFile(log, "");
            var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-Xmx384m", "-jar", FixtureConfig.jar(config.repository(), name).toString(),
                    "--server.address=127.0.0.1", "--server.port=" + port, "--warexis.messaging.enabled=true",
                    "--spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers());
            builder.environment().put("WAREXIS_OAUTH2_ISSUER_URI", issuer());
            builder.environment().put("WAREXIS_OAUTH2_AUDIENCE", "warexis-api");
            builder.environment().put(name.toUpperCase() + "_DB_URL", database.getJdbcUrl());
            builder.environment().put(name.toUpperCase() + "_DB_USERNAME", database.getUsername());
            builder.environment().put(name.toUpperCase() + "_DB_PASSWORD", database.getPassword());
            var process = builder.redirectErrorStream(true).redirectOutput(log.toFile()).start();
            processes.add(process);
            String url = "http://127.0.0.1:" + port;
            awaitHealth(url, process);
            services.put(name, url);
        }
        var env = Map.of("VITE_KEYCLOAK_URL", base, "VITE_KEYCLOAK_REALM", "warexis",
                "VITE_KEYCLOAK_CLIENT_ID", "warexis-operations", "WAREXIS_ORDER_PROXY_TARGET", services.get("order"),
                "WAREXIS_INVENTORY_PROXY_TARGET", services.get("inventory"),
                "WAREXIS_FULFILLMENT_PROXY_TARGET", services.get("fulfillment"));
        runtimePublication.publish(JSON.writeValueAsString(Map.of("status", "READY", "issuer", issuer(),
                "browserOrigin", "http://127.0.0.1:5173", "services", services, "users", users,
                "frontendEnvironment", env, "stopFile", stopFile().toString())));
        System.out.println("Disposable browser stack READY. Browser origin: http://127.0.0.1:5173");
        System.out.println("Identity URL: " + base);
        services.forEach((name, url) -> System.out.println(name + " URL: " + url));
    }

    Path stopFile() { return config.runtime().resolveSibling(config.runtime().getFileName() + ".stop"); }
    String issuer() { return base + "/realms/warexis"; }

    private Map<String, String> user(String admin, String kind, String role) throws Exception {
        String username = kind + "-" + UUID.randomUUID();
        String password = UUID.randomUUID() + "Aa1!";
        request("POST", base + "/admin/realms/warexis/users", Map.of("username", username, "enabled", true,
                "firstName", "Browser", "lastName", kind, "email", username + "@example.invalid",
                "emailVerified", true, "requiredActions", List.of(), "credentials", List.of(Map.of("type", "password",
                        "value", password, "temporary", false))), admin, 201);
        var users = request("GET", base + "/admin/realms/warexis/users?exact=true&username=" + encode(username), null, admin, 200);
        if (users.size() != 1) throw new IllegalStateException("User provisioning did not produce exactly one user");
        var roleJson = request("GET", base + "/admin/realms/warexis/roles/" + role, null, admin, 200);
        var roleRepresentation = JSON.readValue(roleJson.toString(), Map.class);
        request("POST", base + "/admin/realms/warexis/users/" + users.get(0).path("id").asString()
                + "/role-mappings/realm", List.of(roleRepresentation), admin, 204);
        var assigned = request("GET", base + "/admin/realms/warexis/users/" + users.get(0).path("id").asString()
                + "/role-mappings/realm", null, admin, 200);
        boolean expectedRole = false;
        for (var assignment : assigned) if (role.equals(assignment.path("name").asString())) expectedRole = true;
        if (!expectedRole) throw new IllegalStateException("Browser user realm role assignment failed");
        return Map.of("username", username, "password", password);
    }

    private String bootstrap(String secret) throws Exception {
        var body = "grant_type=client_credentials&client_id=browser-bootstrap&client_secret=" + encode(secret);
        var response = HTTP.send(HttpRequest.newBuilder(URI.create(base + "/realms/master/protocol/openid-connect/token"))
                .timeout(Duration.ofSeconds(10)).header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) throw new IllegalStateException("Identity bootstrap failed");
        return JSON.readTree(response.body()).path("access_token").asString();
    }

    private static JsonNode request(String method, String url, Object body, String token, int expected) throws Exception {
        var request = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10)).header("Content-Type", "application/json");
        if (token != null) request.header("Authorization", "Bearer " + token);
        var response = HTTP.send(request.method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body))).build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != expected) throw new IllegalStateException("Identity management returned unexpected HTTP status " + response.statusCode());
        return response.body().isBlank() ? JSON.nullNode() : JSON.readTree(response.body());
    }

    private static void loopback(GenericContainer<?> container) {
        container.setPortBindings(container.getExposedPorts().stream().map(port -> "127.0.0.1::" + port).toList());
    }

    private static String encode(String value) { return URLEncoder.encode(value, StandardCharsets.UTF_8); }
    private static int freePort() throws Exception { try (var socket = new ServerSocket(0)) { return socket.getLocalPort(); } }
    private static void awaitHealth(String base, Process process) throws Exception {
        var deadline = Instant.now().plusSeconds(90);
        while (Instant.now().isBefore(deadline)) {
            if (!process.isAlive()) throw new IllegalStateException("Packaged service exited before readiness; inspect private service logs");
            try {
                var response = HTTP.send(HttpRequest.newBuilder(URI.create(base + "/actuator/health"))
                        .timeout(Duration.ofSeconds(3)).GET().build(), HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200 && "UP".equals(JSON.readTree(response.body()).path("status").asString())) return;
            } catch (java.io.IOException ignored) { /* readiness retries are bounded */ }
            Thread.sleep(250);
        }
        throw new IllegalStateException("Packaged service readiness timed out");
    }

    private static void privateDirectory(Path directory) throws Exception {
        FixtureConfig.requireOutsideGit(directory);
        if (!Files.exists(directory)) Files.createDirectories(directory, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
        if (!Files.getPosixFilePermissions(directory).equals(PosixFilePermissions.fromString("rwx------")))
            throw new IllegalArgumentException("Runtime directory must have private 0700 permissions");
    }

    private static void privateFile(Path path, String content) throws Exception {
        Files.createFile(path, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
        Files.writeString(path, content);
    }

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        List<Exception> failures = new ArrayList<>();
        for (var process : processes.reversed()) {
            try {
                process.destroy();
                if (!process.waitFor(10, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                    if (!process.waitFor(5, TimeUnit.SECONDS)) throw new IllegalStateException("Owned service did not stop");
                }
            } catch (Exception failure) { failures.add(failure); }
        }
        for (var resource : resources.reversed()) {
            try { resource.close(); } catch (Exception failure) { failures.add(failure); }
        }
        try { network.close(); } catch (Exception failure) { failures.add(failure); }
        try { runtimePublication.close(); } catch (Exception failure) { failures.add(failure); }
        if (!failures.isEmpty()) throw new IllegalStateException("Disposable fixture cleanup failed for " + failures.size() + " owned resources");
    }
}
