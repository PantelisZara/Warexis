package io.github.panteliszara.warexis.inventory.reservations.domain;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** One immutable decision for the original order, including rejected requests. */
public record InventoryReservation(UUID reservationId, UUID orderId, UUID sourceEventId,
                                   Status status, ReasonCode reasonCode, Instant decidedAt,
                                   List<ReservationLine> lines) {
    public enum Status { RESERVED, REJECTED }
    public enum ReasonCode { UNKNOWN_SKU, INSUFFICIENT_STOCK }

    public InventoryReservation {
        Objects.requireNonNull(reservationId);
        Objects.requireNonNull(orderId);
        Objects.requireNonNull(sourceEventId);
        Objects.requireNonNull(status);
        Objects.requireNonNull(decidedAt);
        lines = List.copyOf(lines);
        if ((status == Status.REJECTED) != (reasonCode != null)) {
            throw new IllegalArgumentException("Only rejected reservations require a reason code.");
        }
    }
}
