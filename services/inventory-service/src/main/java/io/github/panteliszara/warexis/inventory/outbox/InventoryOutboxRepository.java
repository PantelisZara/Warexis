package io.github.panteliszara.warexis.inventory.outbox;

import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.json.JsonMapper;

import io.github.panteliszara.warexis.inventory.reservations.domain.InventoryReservation;

@Repository
public class InventoryOutboxRepository {
    private final JdbcTemplate jdbc;
    private final JsonMapper mapper;

    public InventoryOutboxRepository(JdbcTemplate jdbc, JsonMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    public void append(InventoryReservation reservation) {
        var eventId = UUID.randomUUID();
        var eventType = reservation.status() == InventoryReservation.Status.RESERVED
                ? "InventoryReserved" : "InventoryRejected";
        var payload = new LinkedHashMap<String, Object>();
        payload.put("orderId", reservation.orderId());
        payload.put("reservationId", reservation.reservationId());
        payload.put("lines", reservation.lines());
        if (reservation.reasonCode() != null) {
            payload.put("reasonCode", reservation.reasonCode().name());
        }
        var envelope = new LinkedHashMap<String, Object>();
        envelope.put("eventId", eventId);
        envelope.put("eventType", eventType);
        envelope.put("eventVersion", 1);
        envelope.put("occurredAt", reservation.decidedAt().toString());
        envelope.put("aggregateId", reservation.reservationId());
        envelope.put("correlationId", reservation.orderId());
        envelope.put("causationId", reservation.sourceEventId());
        envelope.put("payload", payload);
        jdbc.update("""
                INSERT INTO outbox_events
                (event_id, aggregate_id, event_type, event_version, occurred_at, correlation_id, causation_id, event_json)
                VALUES (?, ?, ?, 1, ?, ?, ?, CAST(? AS jsonb))
                """, eventId, reservation.reservationId(), eventType, Timestamp.from(reservation.decidedAt()),
                reservation.orderId(), reservation.sourceEventId(), mapper.writeValueAsString(envelope));
    }
}
