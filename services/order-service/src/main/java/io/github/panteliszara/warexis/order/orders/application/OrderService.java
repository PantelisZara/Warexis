package io.github.panteliszara.warexis.order.orders.application;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import io.github.panteliszara.warexis.order.orders.domain.Order;
import io.github.panteliszara.warexis.order.orders.domain.OrderLineInput;
import io.github.panteliszara.warexis.order.orders.persistence.OrderRepository;

@Service
public class OrderService {

    private final OrderRepository orders;

    public OrderService(OrderRepository orders) {
        this.orders = orders;
    }

    @Transactional
    public OrderView create(List<OrderLineInput> lines) {
        return OrderView.from(orders.saveAndFlush(Order.create(lines, Instant.now())));
    }

    @Transactional(readOnly = true)
    public OrderView get(UUID id) {
        return OrderView.from(orders.findById(id).orElseThrow(OrderNotFoundException::new));
    }
}
