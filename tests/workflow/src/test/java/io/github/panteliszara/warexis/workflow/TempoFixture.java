package io.github.panteliszara.warexis.workflow;

import com.github.dockerjava.api.model.Capability;
import java.net.URI;
import java.net.http.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.BindMode;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import static org.assertj.core.api.Assertions.assertThat;

/** Test-owned, loopback-only real Tempo with bounded ephemeral storage lifetime. */
final class TempoFixture implements AutoCloseable {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final GenericContainer<?> container = new GenericContainer<>(
            "grafana/tempo:3.1.0@sha256:3076b8dcdfb32fd6bc5ccef85e7b7313e6199b9cb84366257fc17ecb696db5fd")
            .withFileSystemBind(Path.of("../../infrastructure/tracing/tempo.yaml").toAbsolutePath().normalize().toString(),
                    "/etc/tempo.yaml", BindMode.READ_ONLY)
            .withCommand("-target=all", "-config.file=/etc/tempo.yaml")
            .withExposedPorts(3200, 4318)
            .withTmpFs(Map.of("/var/tempo", "rw,uid=10001,gid=10001,mode=0700", "/tmp", "rw"))
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withReadonlyRootfs(true)
                    .withCapDrop(Capability.ALL).withSecurityOpts(List.of("no-new-privileges:true")))
            .waitingFor(Wait.forHttp("/ready").forPort(3200).withStartupTimeout(Duration.ofSeconds(60)));
    private boolean paused;
    void start() {
        container.setPortBindings(container.getExposedPorts().stream().map(port -> "127.0.0.1::" + port).toList());
        container.start();
    }
    String endpoint() { return base(4318) + "/v1/traces"; }
    String query() { return base(3200); }
    private String base(int port) { return "http://127.0.0.1:" + container.getMappedPort(port); }
    void pause() {
        org.testcontainers.DockerClientFactory.instance().client().pauseContainerCmd(container.getContainerId()).exec();
        paused = true;
    }
    void resume() {
        if (paused) {
            org.testcontainers.DockerClientFactory.instance().client().unpauseContainerCmd(container.getContainerId()).exec();
            paused = false;
        }
    }
    List<TraceSpan> trace(String traceId, String label, String... sensitive) throws java.io.IOException, InterruptedException {
        assertThat(traceId).matches("[0-9a-f]{32}");
        var response = HTTP.send(HttpRequest.newBuilder(URI.create(query() + "/api/traces/" + traceId))
                .timeout(Duration.ofSeconds(4)).header("Accept", "application/json").GET().build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 404) throw new IncompleteTrace();
        assertThat(response.statusCode()).as("actual Tempo trace query").isEqualTo(200);
        String raw = response.body();
        for (String value : sensitive) if (value != null && !value.isEmpty())
            assertThat(raw.contains(value)).as("sensitive value excluded from stored telemetry").isFalse();
        for (String value : List.of("/home/", "/run/media/", "exception.message", "exception.stacktrace",
                "process.command_line", "host.name", "db.statement", "url.full", "Authorization", "Bearer "))
            assertThat(raw.contains(value)).as("private field excluded from stored telemetry").isFalse();
        var result = new ArrayList<TraceSpan>();
        for (var batch : JSON.readTree(raw).path("batches")) {
            assertThat(batch.path("schemaUrl").asString("")).isEmpty();
            var resource = batch.path("resource").path("attributes");
            assertThat(resource.size()).isEqualTo(1);
            assertThat(resource.get(0).path("key").asString()).isEqualTo("service.name");
            String service = resource.get(0).path("value").path("stringValue").asString();
            assertThat(service).isIn("order-service", "inventory-service", "fulfillment-service");
            for (var scope : batch.path("scopeSpans")) {
                assertThat(scope.path("scope").path("name").asString()).isEqualTo("warexis.tracing");
                assertThat(scope.path("scope").path("version").asString()).isEqualTo("1");
                assertThat(scope.path("scope").path("attributes").size()).isZero();
                assertThat(scope.path("schemaUrl").asString("")).isEmpty();
                for (var span : scope.path("spans")) {
                    assertThat(span.path("name").asString()).isIn("http.request", "warexis.operation",
                            "warexis.outbox.publish", "warexis.kafka.process", "warexis.kafka.dlt", "warexis.fulfillment.dispatch");
                    assertThat(span.path("events").size()).isZero();
                    assertThat(span.path("traceState").asString("")).isEmpty();
                    assertThat(span.path("status").path("message").asString("")).isEmpty();
                    assertThat(span.path("status").path("code").asString("")).isIn("", "STATUS_CODE_UNSET", "STATUS_CODE_ERROR");
                    for (var attribute : span.path("attributes")) {
                        String key = attribute.path("key").asString();
                        var value = attribute.path("value");
                        switch (key) {
                            case "warexis.operation" -> assertThat(value.path("stringValue").asString()).isIn("outbox.publish", "kafka.process", "kafka.dlt", "fulfillment.dispatch");
                            case "warexis.outcome" -> assertThat(value.path("stringValue").asString()).isIn("success", "failure");
                            case "warexis.event_type" -> assertThat(value.path("stringValue").asString()).isIn("OrderPlaced", "InventoryReserved", "InventoryRejected", "OrderConfirmed", "FulfillmentShipped", "unknown");
                            case "http.request.method" -> assertThat(value.path("stringValue").asString()).isIn("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "CONNECT", "TRACE");
                            case "http.response.status_code" -> assertThat(value.path("intValue").asInt()).isBetween(100, 599);
                            default -> throw new AssertionError("Unapproved exported attribute key");
                        }
                    }
                    var links = new ArrayList<Link>();
                    assertThat(span.path("links").size()).isLessThanOrEqualTo(1);
                    for (var link : span.path("links")) {
                        assertThat(link.path("attributes").size()).isZero();
                        assertThat(link.path("traceState").asString("")).isEmpty();
                        links.add(new Link(id(link.path("traceId").asString()), id(link.path("spanId").asString())));
                    }
                    long start = Long.parseLong(span.path("startTimeUnixNano").asString());
                    long end = Long.parseLong(span.path("endTimeUnixNano").asString());
                    assertThat(end).isGreaterThanOrEqualTo(start);
                    result.add(new TraceSpan(service, id(span.path("traceId").asString()), id(span.path("spanId").asString()),
                            id(span.path("parentSpanId").asString("")), span.path("name").asString(),
                            span.path("kind").asString(), start, end, List.copyOf(links),
                            outcome(span), span.path("status").path("code").asString("")));
                }
            }
        }
        Path directory = Path.of("target/tracing-evidence");
        Files.createDirectories(directory);
        Files.writeString(directory.resolve(label + "-" + traceId + ".json"), raw);
        return result;
    }
    private static String outcome(JsonNode span) {
        for (var attribute : span.path("attributes"))
            if (attribute.path("key").asString().equals("warexis.outcome"))
                return attribute.path("value").path("stringValue").asString();
        return "";
    }
    static String id(String encoded) {
        if (encoded.isEmpty() || encoded.matches("[0-9a-f]{16}|[0-9a-f]{32}")) return encoded;
        return HexFormat.of().formatHex(Base64.getDecoder().decode(encoded));
    }
    static TraceSpan child(List<TraceSpan> spans, String service, String name, String parent) {
        return spans.stream().filter(s -> s.service().equals(service) && s.name().equals(name) && s.parent().equals(parent))
                .findFirst().orElseThrow(() -> new IncompleteTrace());
    }
    static final class IncompleteTrace extends RuntimeException { }
    static TraceSpan byId(List<TraceSpan> spans, String id, String service, String name) {
        var span = spans.stream().filter(s -> s.id().equals(id)).findFirst().orElseThrow(IncompleteTrace::new);
        assertThat(span.service()).isEqualTo(service);
        assertThat(span.name()).isEqualTo(name);
        return span;
    }
    static void descendsFromHttp(List<TraceSpan> spans, TraceSpan child) {
        var seen = new HashSet<String>();
        while (seen.add(child.id())) {
            if (child.name().equals("http.request") && child.kind().equals("SPAN_KIND_SERVER")) return;
            String parent = child.parent();
            child = spans.stream().filter(s -> s.id().equals(parent)).findFirst()
                    .orElseThrow(() -> new IncompleteTrace());
        }
        throw new AssertionError("Cycle in exported span ancestry");
    }
    @Override public void close() { resume(); container.stop(); }
    record Link(String trace, String span) { }
    record TraceSpan(String service, String trace, String id, String parent, String name, String kind,
                     long start, long end, List<Link> links, String outcome, String status) { }
}
