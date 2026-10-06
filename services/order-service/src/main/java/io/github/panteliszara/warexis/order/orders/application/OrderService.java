package io.github.panteliszara.warexis.order.orders.application;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import io.github.panteliszara.warexis.order.outbox.OutboxEvent;
import io.github.panteliszara.warexis.order.outbox.OutboxRepository;
import io.github.panteliszara.warexis.order.orders.domain.Order;
import io.github.panteliszara.warexis.order.orders.domain.OrderLineInput;
import io.github.panteliszara.warexis.order.orders.persistence.OrderRepository;

@Service
public class OrderService {

    private final OrderRepository orders;

    private final OutboxRepository outbox;
    private final ObjectMapper objectMapper;

    public OrderService(OrderRepository orders, OutboxRepository outbox, ObjectMapper objectMapper) {
        this.orders = orders;
        this.outbox = outbox;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public OrderView create(List<OrderLineInput> lines) {
        var order = orders.saveAndFlush(Order.create(lines, Instant.now()));
        outbox.append(OutboxEvent.orderPlaced(order, objectMapper));
        return OrderView.from(order);
    }

    @Transactional(readOnly = true)
    public OrderView get(UUID id) {
        return OrderView.from(orders.findById(id).orElseThrow(OrderNotFoundException::new));
    }
}
