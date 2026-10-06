package io.github.panteliszara.warexis.inventory.reservations.domain;

import java.util.Objects;
import java.util.UUID;

import io.github.panteliszara.warexis.inventory.stock.domain.StockItem;

public record ReservationLine(UUID orderLineId, String sku, int quantity) {
    public ReservationLine {
        Objects.requireNonNull(orderLineId, "Order line ID is required.");
        if (sku == null || !sku.matches(StockItem.SKU_PATTERN)) {
            throw new IllegalArgumentException("Invalid SKU.");
        }
        if (quantity <= 0) {
            throw new IllegalArgumentException("Quantity must be a positive integer.");
        }
    }
}
