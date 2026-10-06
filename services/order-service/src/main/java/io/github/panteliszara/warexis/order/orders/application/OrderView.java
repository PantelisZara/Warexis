package io.github.panteliszara.warexis.order.orders.application;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import io.github.panteliszara.warexis.order.orders.domain.Order;
import io.github.panteliszara.warexis.order.orders.domain.OrderStatus;

public record OrderView(UUID id, OrderStatus status, Instant createdAt, Instant updatedAt, List<Line> lines) {

    static OrderView from(Order order) {
        return new OrderView(order.getId(), order.getStatus(), order.getCreatedAt(), order.getUpdatedAt(),
                order.getLines().stream().map(line -> new Line(line.getId(), line.getSku(), line.getQuantity())).toList());
    }

    public record Line(UUID id, String sku, int quantity) {
    }
}
