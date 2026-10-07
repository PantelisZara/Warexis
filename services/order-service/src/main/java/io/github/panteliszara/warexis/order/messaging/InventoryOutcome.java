package io.github.panteliszara.warexis.order.messaging;

import java.time.Instant;
import java.util.List;
import java.util.HashSet;
import java.util.Objects;
import java.util.UUID;
import tools.jackson.databind.ObjectMapper;

public record InventoryOutcome(UUID eventId, String eventType, Instant occurredAt, UUID aggregateId,
        UUID correlationId, UUID causationId, UUID orderId, UUID reservationId, List<Line> lines, String reasonCode) {
    public InventoryOutcome {
        if (eventId == null || occurredAt == null || aggregateId == null || correlationId == null
                || causationId == null || orderId == null || reservationId == null
                || !Objects.equals(correlationId, orderId) || !Objects.equals(aggregateId, reservationId)
                || (!Objects.equals(eventType, "InventoryReserved") && !Objects.equals(eventType, "InventoryRejected"))
                || (Objects.equals(eventType, "InventoryReserved") && reasonCode != null)
                || (Objects.equals(eventType, "InventoryRejected") && !Objects.equals(reasonCode, "UNKNOWN_SKU")
                    && !Objects.equals(reasonCode, "INSUFFICIENT_STOCK"))
                || lines == null || lines.isEmpty() || lines.size() > 100) {
            throw new PoisonMessageException();
        }
        var ids = new HashSet<UUID>();
        for (var line : lines) {
            if (line == null || line.orderLineId() == null || !ids.add(line.orderLineId())
                    || line.sku() == null || !line.sku().matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
                    || line.quantity() <= 0) {
                throw new PoisonMessageException();
            }
        }
        lines = List.copyOf(lines);
    }

    public record Line(UUID orderLineId, String sku, int quantity) {
    }

    public String decisionJson(ObjectMapper mapper) {
        return mapper.writeValueAsString(new Decision(eventType, occurredAt.toString(), aggregateId,
                correlationId, causationId, orderId, reservationId, lines, reasonCode));
    }

    private record Decision(String eventType, String occurredAt, UUID aggregateId, UUID correlationId,
            UUID causationId, UUID orderId, UUID reservationId, List<Line> lines, String reasonCode) {
    }
}
