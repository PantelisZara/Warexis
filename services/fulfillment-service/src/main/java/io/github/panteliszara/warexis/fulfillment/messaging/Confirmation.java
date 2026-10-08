package io.github.panteliszara.warexis.fulfillment.messaging;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
public record Confirmation(UUID eventId, String eventType, Instant occurredAt, UUID orderId,
        UUID reservationId, UUID causationId, List<Line> lines) {
    public Confirmation { lines = List.copyOf(lines); }
    public record Line(UUID orderLineId, String sku, int quantity) {}
}
