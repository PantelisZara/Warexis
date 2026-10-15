package io.github.panteliszara.warexis.order.messaging;

import java.time.Instant;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import io.github.panteliszara.warexis.order.orders.domain.OrderStatus;
import io.github.panteliszara.warexis.order.outbox.OutboxEvent;
import io.github.panteliszara.warexis.order.outbox.OutboxRepository;
import io.github.panteliszara.warexis.order.orders.persistence.OrderRepository;

@Service
public class InventoryOutcomeService {
    private final OrderRepository orders;
    private final JdbcClient jdbc;
    private final ObjectMapper mapper;
    private final OutboxRepository outbox;

    public InventoryOutcomeService(OrderRepository orders, JdbcClient jdbc, ObjectMapper mapper, OutboxRepository outbox) {
        this.orders = orders;
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.outbox = outbox;
    }

    @Transactional
    public boolean apply(InventoryOutcome outcome) {
        jdbc.sql("SELECT pg_advisory_xact_lock(92, hashtext(:id))")
                .param("id", outcome.eventId().toString()).query(Object.class).single();
        var incoming = mapper.writeValueAsString(outcome);
        var replay = jdbc.sql("SELECT event_json::text FROM processed_inventory_events WHERE event_id = :id")
                .param("id", outcome.eventId()).query(String.class).optional();
        if (replay.isPresent()) {
            if (!mapper.readTree(replay.get()).equals(mapper.readTree(incoming))) {
                throw new PoisonMessageException();
            }
            return false;
        }
        jdbc.sql("SELECT pg_advisory_xact_lock(93, hashtext(:id))")
                .param("id", outcome.reservationId().toString()).query(Object.class).single();
        var order = orders.findForUpdate(outcome.orderId()).orElseThrow(PoisonMessageException::new);
        var expectedLines = order.getLines().stream().map(line ->
                new InventoryOutcome.Line(line.getId(), line.getSku(), line.getQuantity())).toList();
        var placedId = jdbc.sql("SELECT event_id FROM outbox_events WHERE aggregate_id = :id AND event_type = 'OrderPlaced' AND event_version = 1")
                .param("id", order.getId()).query(java.util.UUID.class).single();
        if (!expectedLines.equals(outcome.lines()) || !placedId.equals(outcome.causationId())
                || !outcome.correlationId().equals(order.getId())
                || !outcome.aggregateId().equals(outcome.reservationId())) {
            throw new PoisonMessageException();
        }
        var decisionJson = outcome.decisionJson(mapper);
        var existing = jdbc.sql("SELECT decision_json::text FROM inventory_decisions WHERE order_id = :id")
                .param("id", order.getId()).query(String.class).optional();
        if (existing.isPresent()) {
            if (!mapper.readTree(existing.get()).equals(mapper.readTree(decisionJson))) {
                throw new PoisonMessageException();
            }
        } else {
            if (order.getStatus() != OrderStatus.PENDING_INVENTORY) {
                throw new PoisonMessageException();
            }
            var reservationOwner = jdbc.sql("SELECT order_id FROM inventory_decisions WHERE reservation_id = :id")
                    .param("id", outcome.reservationId()).query(java.util.UUID.class).optional();
            if (reservationOwner.isPresent()) {
                throw new PoisonMessageException();
            }
            requireOne(jdbc.sql("INSERT INTO inventory_decisions (order_id,reservation_id,decision_json) VALUES (:orderId,:reservationId,CAST(:json AS jsonb))")
                    .param("orderId", order.getId()).param("reservationId", outcome.reservationId())
                    .param("json", decisionJson).update());
            var confirmed = outcome.eventType().equals("InventoryReserved");
            var resolvedAt = Instant.now();
            order.resolveInventory(confirmed, resolvedAt);
            orders.flush();
            if (confirmed) {
                outbox.append(OutboxEvent.orderConfirmed(order, outcome.reservationId(), outcome.eventId(), order.getUpdatedAt(), mapper));
            }
        }
        boolean newDecision = existing.isEmpty();
        requireOne(jdbc.sql("INSERT INTO processed_inventory_events VALUES (:id,:orderId,CAST(:json AS jsonb))")
                .param("id", outcome.eventId()).param("orderId", order.getId()).param("json", incoming).update());
        return newDecision;
    }

    private void requireOne(int rows) {
        if (rows != 1) {
            throw new IllegalStateException("Inventory outcome write did not persist one row.");
        }
    }
}
