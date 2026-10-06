package io.github.panteliszara.warexis.inventory.reservations;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;

import io.github.panteliszara.warexis.inventory.reservations.application.ConflictingOrderPlacedException;
import io.github.panteliszara.warexis.inventory.reservations.application.InventoryReservationService;
import io.github.panteliszara.warexis.inventory.reservations.application.OrderPlaced;
import io.github.panteliszara.warexis.inventory.reservations.domain.InventoryReservation;
import io.github.panteliszara.warexis.inventory.reservations.domain.InventoryReservation.ReasonCode;
import io.github.panteliszara.warexis.inventory.reservations.domain.InventoryReservation.Status;
import io.github.panteliszara.warexis.inventory.reservations.domain.ReservationLine;
import io.github.panteliszara.warexis.inventory.stock.application.StockItemService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
@Testcontainers
class InventoryReservationIntegrationTests {
    @Container
    @ServiceConnection
    static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18.6");

    @Autowired private InventoryReservationService reservations;
    @Autowired private StockItemService stocks;
    @Autowired private JdbcTemplate jdbc;
    @Autowired private DataSource dataSource;
    @Autowired private JsonMapper mapper;

    @Test
    void multiLineSuccessPreservesOriginalLinesAndWritesOneCompleteEnvelope() {
        var a = stock(8);
        var b = stock(9);
        var input = input(line(b, 3), line(a, 2), line(a, 1));
        var result = reservations.process(input);
        assertThat(result.status()).isEqualTo(Status.RESERVED);
        assertThat(result.reasonCode()).isNull();
        assertThat(result.lines()).containsExactlyElementsOf(input.lines());
        assertReserved(a, 3);
        assertReserved(b, 3);
        assertRows(input, 1);
        var event = mapper.readTree(jdbc.queryForObject("SELECT event_json::text FROM outbox_events WHERE aggregate_id = ?", String.class, result.reservationId()));
        assertThat(event.size()).isEqualTo(8);
        UUID.fromString(event.path("eventId").asString());
        assertThat(event.path("eventType").asString()).isEqualTo("InventoryReserved");
        assertThat(event.path("eventVersion").asInt()).isEqualTo(1);
        assertThat(Instant.parse(event.path("occurredAt").asString())).isEqualTo(result.decidedAt());
        assertThat(event.path("aggregateId").asString()).isEqualTo(result.reservationId().toString());
        assertThat(event.path("correlationId").asString()).isEqualTo(input.orderId().toString());
        assertThat(event.path("causationId").asString()).isEqualTo(input.eventId().toString());
        assertThat(event.path("payload").path("orderId").asString()).isEqualTo(input.orderId().toString());
        assertThat(event.path("payload").path("reservationId").asString()).isEqualTo(result.reservationId().toString());
        assertThat(event.path("payload").path("lines")).isEqualTo(mapper.valueToTree(input.lines()));
        assertThat(event.path("payload").has("reasonCode")).isFalse();
        assertThat(jdbc.queryForObject("SELECT published_at IS NULL FROM outbox_events WHERE aggregate_id = ?", Boolean.class, result.reservationId())).isTrue();
    }

    @Test
    void insufficientStockRejectsEveryLineAndPersistsRejectionEnvelope() {
        var a = stock(10);
        var b = stock(1);
        var input = input(line(a, 2), line(b, 2));
        var result = reservations.process(input);
        assertThat(result.status()).isEqualTo(Status.REJECTED);
        assertThat(result.reasonCode()).isEqualTo(ReasonCode.INSUFFICIENT_STOCK);
        assertReserved(a, 0);
        assertReserved(b, 0);
        assertRows(input, 1);
        var event = mapper.readTree(jdbc.queryForObject("SELECT event_json::text FROM outbox_events WHERE aggregate_id = ?", String.class, result.reservationId()));
        assertThat(event.path("eventType").asString()).isEqualTo("InventoryRejected");
        assertThat(event.path("payload").path("reasonCode").asString()).isEqualTo("INSUFFICIENT_STOCK");
        assertThat(event.path("payload").path("lines")).isEqualTo(mapper.valueToTree(input.lines()));
    }

    @Test
    void unknownSkuTakesPrecedenceAndLeavesKnownStockUntouched() {
        var a = stock(1);
        var input = input(line(a, 2), line("Missing-" + UUID.randomUUID(), 1));
        var result = reservations.process(input);
        assertThat(result.reasonCode()).isEqualTo(ReasonCode.UNKNOWN_SKU);
        assertReserved(a, 0);
        assertRows(input, 1);
        assertThat(jdbc.queryForObject("SELECT event_json -> 'payload' ->> 'reasonCode' FROM outbox_events WHERE aggregate_id = ?", String.class, result.reservationId())).isEqualTo("UNKNOWN_SKU");
    }

    @Test
    void repeatedSkuDemandIsAggregatedAndCannotOverflow() {
        var a = stock(5);
        assertThat(reservations.process(input(line(a, 3), line(a, 3))).reasonCode()).isEqualTo(ReasonCode.INSUFFICIENT_STOCK);
        assertReserved(a, 0);
        var b = stock(Integer.MAX_VALUE);
        var result = reservations.process(input(line(b, Integer.MAX_VALUE), line(b, Integer.MAX_VALUE)));
        assertThat(result.reasonCode()).isEqualTo(ReasonCode.INSUFFICIENT_STOCK);
        assertReserved(b, 0);
    }

    @Test
    void exactDuplicateAndEquivalentOrderUnderNewEventReuseOriginalDecision() {
        var a = stock(10);
        var input = input(line(a, 4));
        var original = reservations.process(input);
        assertThat(reservations.process(input)).isEqualTo(original);
        var alias = new OrderPlaced(UUID.randomUUID(), input.orderId(), input.lines());
        assertThat(reservations.process(alias)).isEqualTo(original);
        assertReserved(a, 4);
        assertRows(input, 2);
        assertThat(jdbc.queryForObject("SELECT causation_id FROM outbox_events WHERE aggregate_id = ?", UUID.class, original.reservationId())).isEqualTo(input.eventId());
    }

    @Test
    void rejectedDecisionRemainsRejectedAfterStockArrives() {
        var a = stock(0);
        var input = input(line(a, 1));
        var rejected = reservations.process(input);
        stocks.update(a, 5);
        assertThat(reservations.process(input)).isEqualTo(rejected);
        assertThat(reservations.process(new OrderPlaced(UUID.randomUUID(), input.orderId(), input.lines()))).isEqualTo(rejected);
        assertReserved(a, 0);
        assertRows(input, 2);
    }

    @Test
    void conflictingEventIdentityOrOrderLinesAreUnprocessableAndChangeNothing() {
        var a = stock(10);
        var input = input(line(a, 2));
        var original = reservations.process(input);
        assertThatThrownBy(() -> reservations.process(new OrderPlaced(input.eventId(), UUID.randomUUID(), input.lines())))
                .isInstanceOf(ConflictingOrderPlacedException.class);
        var changed = List.of(new ReservationLine(input.lines().getFirst().orderLineId(), a, 3));
        assertThatThrownBy(() -> reservations.process(new OrderPlaced(input.eventId(), input.orderId(), changed)))
                .isInstanceOf(ConflictingOrderPlacedException.class);
        assertThatThrownBy(() -> reservations.process(new OrderPlaced(UUID.randomUUID(), input.orderId(), changed)))
                .isInstanceOf(ConflictingOrderPlacedException.class);
        assertThat(reservations.process(input)).isEqualTo(original);
        assertReserved(a, 2);
        assertRows(input, 1);
    }

    @Test
    void concurrentOrdersCannotOversellAndOppositeLineOrderDoesNotDeadlock() throws Exception {
        var a = stock(5);
        var b = stock(5);
        var outcomes = concurrent(input(line(a, 4), line(b, 4)), input(line(b, 4), line(a, 4)));
        assertThat(outcomes).extracting(InventoryReservation::status).containsExactlyInAnyOrder(Status.RESERVED, Status.REJECTED);
        assertReserved(a, 4);
        assertReserved(b, 4);
    }

    @Test
    void concurrentDuplicateEventAndSameOrderAliasCommitExactlyOneOutcome() throws Exception {
        var a = stock(10);
        var input = input(line(a, 2));
        var duplicate = concurrent(input, input);
        assertThat(duplicate.get(0)).isEqualTo(duplicate.get(1));
        assertRows(input, 1);
        var b = stock(10);
        var other = input(line(b, 3));
        var alias = new OrderPlaced(UUID.randomUUID(), other.orderId(), other.lines());
        var sameOrder = concurrent(other, alias);
        assertThat(sameOrder.get(0)).isEqualTo(sameOrder.get(1));
        assertReserved(a, 2);
        assertReserved(b, 3);
        assertRows(other, 2);
    }

    @Test
    void stockLocksUseCaseSensitiveCOrderingEvenWhenInputIsReversed() throws Exception {
        var suffix = UUID.randomUUID().toString();
        var upper = "A-" + suffix;
        var lower = "a-" + suffix;
        stocks.create(upper, 3);
        stocks.create(lower, 3);
        var input = input(line(lower, 1), line(upper, 1));
        try (Connection blocker = dataSource.getConnection(); var executor = Executors.newSingleThreadExecutor()) {
            blocker.setAutoCommit(false);
            try (var lock = blocker.prepareStatement("SELECT id FROM stock_items WHERE sku = ? FOR UPDATE")) {
                lock.setString(1, upper);
                lock.executeQuery().close();
            }
            var result = executor.submit(() -> reservations.process(input));
            try {
                awaitStockLock();
                // If lowercase had been locked first, this NOWAIT would fail rather than succeed.
                try (var lock = blocker.prepareStatement("SELECT id FROM stock_items WHERE sku = ? FOR UPDATE NOWAIT")) {
                    lock.setString(1, lower);
                    lock.executeQuery().close();
                }
                assertThat(result.isDone()).isFalse();
                blocker.commit();
                assertThat(result.get(10, TimeUnit.SECONDS).status()).isEqualTo(Status.RESERVED);
            } finally {
                blocker.rollback();
            }
        }
        assertReserved(upper, 1);
        assertReserved(lower, 1);
    }

    @Test
    void lateOutboxFailureRollsBackFlushedStockReservationAndProcessedEvent() {
        var a = stock(10);
        var input = input(line(a, 4));
        jdbc.execute("ALTER TABLE outbox_events ADD CONSTRAINT test_force_outbox_failure CHECK (event_type = 'invalid') NOT VALID");
        try {
            assertThatThrownBy(() -> reservations.process(input)).isInstanceOf(DataIntegrityViolationException.class);
        } finally {
            jdbc.execute("ALTER TABLE outbox_events DROP CONSTRAINT test_force_outbox_failure");
        }
        assertReserved(a, 0);
        assertThat(count("SELECT count(*) FROM inventory_reservations WHERE order_id = ?", input.orderId())).isZero();
        assertThat(count("SELECT count(*) FROM processed_order_events WHERE event_id = ?", input.eventId())).isZero();
        assertThat(count("SELECT count(*) FROM outbox_events WHERE correlation_id = ?", input.orderId())).isZero();
        assertThat(reservations.process(input).status()).isEqualTo(Status.RESERVED);
        assertReserved(a, 4);
        assertRows(input, 1);
    }

    @Test
    void databaseRejectsSecondDecisionAndSecondOutboxOutcomeForOneOrder() {
        var input = input(line(stock(1), 1));
        var result = reservations.process(input);
        assertThatThrownBy(() -> jdbc.update("""
                INSERT INTO inventory_reservations (reservation_id, order_id, source_event_id, status, decided_at, lines)
                VALUES (?, ?, ?, 'RESERVED', now(), '[{"orderLineId":"id","sku":"A","quantity":1}]'::jsonb)
                """, UUID.randomUUID(), input.orderId(), UUID.randomUUID())).isInstanceOf(DataIntegrityViolationException.class)
                .rootCause().isInstanceOf(SQLException.class)
                .extracting(cause -> ((SQLException) cause).getSQLState()).isEqualTo("23505");
        var duplicateEventId = UUID.randomUUID();
        assertThatThrownBy(() -> jdbc.update("""
                INSERT INTO outbox_events (event_id, aggregate_id, event_type, event_version, occurred_at, correlation_id, causation_id, event_json)
                SELECT ?, aggregate_id, event_type, event_version, occurred_at, correlation_id, causation_id,
                       jsonb_set(event_json, '{eventId}', to_jsonb(CAST(? AS text)))
                FROM outbox_events WHERE aggregate_id = ?
                """, duplicateEventId, duplicateEventId.toString(), result.reservationId())).isInstanceOf(DataIntegrityViolationException.class)
                .rootCause().isInstanceOf(SQLException.class)
                .extracting(cause -> ((SQLException) cause).getSQLState()).isEqualTo("23505");
        assertRows(input, 1);
    }

    @Test
    void databaseRejectsIncompleteOrContradictoryOutgoingEnvelopeAndPublicationTime() {
        var input = input(line(stock(1), 1));
        var result = reservations.process(input);
        var id = result.reservationId();
        assertThatThrownBy(() -> jdbc.update("UPDATE outbox_events SET event_json = event_json - 'eventId' WHERE aggregate_id = ?", id))
                .isInstanceOf(DataIntegrityViolationException.class);
        assertThatThrownBy(() -> jdbc.update("UPDATE outbox_events SET event_json = jsonb_set(event_json, '{payload,orderId}', to_jsonb(CAST(? AS text))) WHERE aggregate_id = ?", UUID.randomUUID().toString(), id))
                .isInstanceOf(DataIntegrityViolationException.class);
        assertThatThrownBy(() -> jdbc.update("UPDATE outbox_events SET event_json = jsonb_set(event_json, '{occurredAt}', '\"2020-01-01T00:00:00Z\"'::jsonb) WHERE aggregate_id = ?", id))
                .isInstanceOf(DataIntegrityViolationException.class);
        assertThatThrownBy(() -> jdbc.update("UPDATE outbox_events SET event_json = jsonb_set(event_json, '{eventVersion}', '\"1\"'::jsonb) WHERE aggregate_id = ?", id))
                .isInstanceOf(DataIntegrityViolationException.class);
        assertThatThrownBy(() -> jdbc.update("UPDATE outbox_events SET published_at = occurred_at - interval '1 second' WHERE aggregate_id = ?", id))
                .isInstanceOf(DataIntegrityViolationException.class);
        assertThat(reservations.process(input)).isEqualTo(result);
    }

    private List<InventoryReservation> concurrent(OrderPlaced first, OrderPlaced second) throws Exception {
        try (var executor = Executors.newFixedThreadPool(2)) {
            var start = new CountDownLatch(1);
            var ready = new CountDownLatch(2);
            var a = executor.submit(() -> { ready.countDown(); start.await(); return reservations.process(first); });
            var b = executor.submit(() -> { ready.countDown(); start.await(); return reservations.process(second); });
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            return List.of(a.get(15, TimeUnit.SECONDS), b.get(15, TimeUnit.SECONDS));
        }
    }

    private void awaitStockLock() throws Exception {
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (Boolean.TRUE.equals(jdbc.queryForObject("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE '%stock_items%')", Boolean.class))) {
                return;
            }
            Thread.sleep(25);
        }
        throw new AssertionError("Reservation did not wait for the expected PostgreSQL stock row lock.");
    }

    private String stock(int onHand) {
        var sku = "SKU-" + UUID.randomUUID();
        stocks.create(sku, onHand);
        return sku;
    }

    private ReservationLine line(String sku, int quantity) {
        return new ReservationLine(UUID.randomUUID(), sku, quantity);
    }

    private OrderPlaced input(ReservationLine... lines) {
        return new OrderPlaced(UUID.randomUUID(), UUID.randomUUID(), List.of(lines));
    }

    private void assertReserved(String sku, int expected) {
        var item = stocks.get(sku);
        assertThat(item.getReserved()).isEqualTo(expected).isBetween(0, item.getOnHand());
    }

    private int count(String query, Object parameter) {
        return jdbc.queryForObject(query, Integer.class, parameter);
    }

    private void assertRows(OrderPlaced input, int processedCount) {
        assertThat(count("SELECT count(*) FROM inventory_reservations WHERE order_id = ?", input.orderId())).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM processed_order_events p JOIN inventory_reservations r USING (reservation_id) WHERE r.order_id = ?", input.orderId())).isEqualTo(processedCount);
        assertThat(count("SELECT count(*) FROM outbox_events WHERE correlation_id = ?", input.orderId())).isEqualTo(1);
    }
}
