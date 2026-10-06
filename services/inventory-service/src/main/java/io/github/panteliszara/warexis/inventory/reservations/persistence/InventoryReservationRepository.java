package io.github.panteliszara.warexis.inventory.reservations.persistence;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import io.github.panteliszara.warexis.inventory.reservations.domain.InventoryReservation;
import io.github.panteliszara.warexis.inventory.reservations.domain.ReservationLine;

@Repository
public class InventoryReservationRepository {
    private final JdbcTemplate jdbc;
    private final JsonMapper mapper;

    public InventoryReservationRepository(JdbcTemplate jdbc, JsonMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    /** Separate lock namespaces prevent event/order identity inversion; hash collisions only serialize work. */
    public void lockInput(UUID eventId, UUID orderId) {
        lock(1, eventId);
        lock(2, orderId);
    }

    private void lock(int namespace, UUID id) {
        jdbc.execute((ConnectionCallback<Void>) connection -> {
            try (var statement = connection.prepareStatement("SELECT pg_advisory_xact_lock(?, hashtext(?))")) {
                statement.setInt(1, namespace);
                statement.setString(2, id.toString());
                statement.execute();
            }
            return null;
        });
    }

    public Optional<InventoryReservation> findByEventId(UUID eventId) {
        return jdbc.query("""
                SELECT r.* FROM inventory_reservations r
                JOIN processed_order_events p ON p.reservation_id = r.reservation_id
                WHERE p.event_id = ?
                """, this::read, eventId).stream().findFirst();
    }

    public Optional<InventoryReservation> findByOrderId(UUID orderId) {
        return jdbc.query("SELECT * FROM inventory_reservations WHERE order_id = ?", this::read, orderId)
                .stream().findFirst();
    }

    public void save(InventoryReservation reservation) {
        jdbc.update("""
                INSERT INTO inventory_reservations
                (reservation_id, order_id, source_event_id, status, reason_code, decided_at, lines)
                VALUES (?, ?, ?, ?, ?, ?, CAST(? AS jsonb))
                """, reservation.reservationId(), reservation.orderId(), reservation.sourceEventId(),
                reservation.status().name(), reservation.reasonCode() == null ? null : reservation.reasonCode().name(),
                Timestamp.from(reservation.decidedAt()), mapper.writeValueAsString(reservation.lines()));
    }

    public void markProcessed(UUID eventId, InventoryReservation reservation) {
        jdbc.update("INSERT INTO processed_order_events (event_id, reservation_id, processed_at) VALUES (?, ?, ?)",
                eventId, reservation.reservationId(), Timestamp.from(Instant.now()));
    }

    private InventoryReservation read(ResultSet row, int index) throws SQLException {
        return new InventoryReservation(row.getObject("reservation_id", UUID.class),
                row.getObject("order_id", UUID.class), row.getObject("source_event_id", UUID.class),
                InventoryReservation.Status.valueOf(row.getString("status")),
                row.getString("reason_code") == null ? null : InventoryReservation.ReasonCode.valueOf(row.getString("reason_code")),
                row.getTimestamp("decided_at").toInstant(),
                mapper.readValue(row.getString("lines"), new TypeReference<java.util.List<ReservationLine>>() {}));
    }
}
