package io.github.panteliszara.warexis.order.outbox;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import tools.jackson.databind.ObjectMapper;

import io.github.panteliszara.warexis.order.orders.domain.Order;

public record OutboxEvent(UUID eventId, UUID aggregateId, String eventType, int eventVersion,
        Instant occurredAt, UUID correlationId, String eventJson) {

    public static OutboxEvent orderPlaced(Order order, ObjectMapper mapper) {
        var eventId = UUID.randomUUID();
        var lines = order.getLines().stream()
                .map(line -> new Line(line.getId(), line.getSku(), line.getQuantity())).toList();
        var envelope = new Envelope(eventId, "OrderPlaced", 1, order.getCreatedAt().toString(),
                order.getId(), order.getId(), new Payload(order.getId(), lines));
        return new OutboxEvent(eventId, order.getId(), envelope.eventType(), envelope.eventVersion(),
                order.getCreatedAt(), order.getId(), mapper.writeValueAsString(envelope));
    }

    @JsonPropertyOrder({"eventId", "eventType", "eventVersion", "occurredAt", "aggregateId", "correlationId", "payload"})
    private record Envelope(UUID eventId, String eventType, int eventVersion, String occurredAt,
            UUID aggregateId, UUID correlationId, Payload payload) {
    }

    @JsonPropertyOrder({"orderId", "lines"})
    private record Payload(UUID orderId, List<Line> lines) {
    }

    @JsonPropertyOrder({"orderLineId", "sku", "quantity"})
    private record Line(UUID orderLineId, String sku, int quantity) {
    }
}
