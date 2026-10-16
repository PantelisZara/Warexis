package io.github.panteliszara.warexis.inventory.observability;

import java.sql.DriverManager;
import java.util.LinkedHashMap;
import java.util.UUID;
import tools.jackson.databind.json.JsonMapper;

/** Separate process reads only committed PostgreSQL state; no inherited request/thread context. */
public final class InventoryTraceRestartReader {
    public static void main(String[] args) throws Exception {
        var mapper = JsonMapper.builder().build();
        var input = mapper.readTree(System.in.readAllBytes());
        try (var connection = DriverManager.getConnection(input.path("url").asString(), input.path("user").asString(), input.path("password").asString());
                var query = connection.prepareStatement("SELECT event_id, event_json::text, traceparent, correlation_id FROM outbox_events WHERE correlation_id = ?")) {
            query.setObject(1, UUID.fromString(input.path("orderId").asString()));
            try (var row = query.executeQuery()) {
                if (!row.next()) throw new IllegalStateException("Committed outbox row missing.");
                var result = new LinkedHashMap<String, Object>();
                result.put("pid", ProcessHandle.current().pid());
                result.put("eventId", row.getString("event_id"));
                result.put("payload", row.getString("event_json"));
                result.put("traceparent", row.getString("traceparent"));
                result.put("key", row.getString("correlation_id"));
                System.out.write(mapper.writeValueAsBytes(result));
            }
        }
    }
}
