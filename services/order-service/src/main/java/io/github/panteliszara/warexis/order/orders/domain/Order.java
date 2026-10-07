package io.github.panteliszara.warexis.order.orders.domain;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OrderBy;
import jakarta.persistence.Table;

@Entity
@Table(name = "orders")
public class Order {

    @Id
    private UUID id;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private OrderStatus status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL)
    @OrderBy("linePosition ASC")
    private List<OrderLine> lines = new ArrayList<>();

    protected Order() {
    }

    public static Order create(List<OrderLineInput> input, Instant now) {
        if (input == null || input.isEmpty() || input.size() > 100 || input.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("An order must contain 1 to 100 non-null lines.");
        }
        var order = new Order();
        order.id = UUID.randomUUID();
        order.status = OrderStatus.PENDING_INVENTORY;
        order.createdAt = Objects.requireNonNull(now).truncatedTo(ChronoUnit.MICROS);
        order.updatedAt = order.createdAt;
        for (int position = 0; position < input.size(); position++) {
            order.lines.add(new OrderLine(order, input.get(position), position));
        }
        return order;
    }

    public void resolveInventory(boolean reserved, Instant now) {
        if (status != OrderStatus.PENDING_INVENTORY) {
            throw new IllegalStateException("Order already has an inventory decision.");
        }
        status = reserved ? OrderStatus.CONFIRMED : OrderStatus.REJECTED;
        updatedAt = Objects.requireNonNull(now).truncatedTo(ChronoUnit.MICROS);
        if (updatedAt.isBefore(createdAt)) {
            updatedAt = createdAt;
        }
    }

    public UUID getId() {
        return id;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public List<OrderLine> getLines() {
        return List.copyOf(lines);
    }
}
