package io.github.panteliszara.warexis.order.orders.persistence;

import java.util.UUID;

import java.util.Optional;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;

import io.github.panteliszara.warexis.order.orders.domain.Order;

public interface OrderRepository extends JpaRepository<Order, UUID> {
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT o FROM Order o WHERE o.id = :id")
    Optional<Order> findForUpdate(UUID id);
}
