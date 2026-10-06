package io.github.panteliszara.warexis.order.orders.persistence;

import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;

import io.github.panteliszara.warexis.order.orders.domain.Order;

public interface OrderRepository extends JpaRepository<Order, UUID> {
}
