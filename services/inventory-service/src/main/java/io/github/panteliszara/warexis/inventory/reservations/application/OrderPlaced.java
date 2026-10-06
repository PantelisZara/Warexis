package io.github.panteliszara.warexis.inventory.reservations.application;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

import io.github.panteliszara.warexis.inventory.reservations.domain.ReservationLine;

/** Validated business input; a future transport adapter validates the event envelope separately. */
public record OrderPlaced(UUID eventId, UUID orderId, List<ReservationLine> lines) {
    public OrderPlaced {
        Objects.requireNonNull(eventId, "Event ID is required.");
        Objects.requireNonNull(orderId, "Order ID is required.");
        if (lines == null || lines.isEmpty() || lines.size() > 100 || lines.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("An order must contain 1 to 100 non-null lines.");
        }
        lines = List.copyOf(lines);
        if (lines.stream().map(ReservationLine::orderLineId).distinct().count() != lines.size()) {
            throw new IllegalArgumentException("Order line IDs must be unique within the order.");
        }
    }
}
