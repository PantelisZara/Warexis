package io.github.panteliszara.warexis.inventory.reservations;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import io.github.panteliszara.warexis.inventory.reservations.application.InventoryReservationService;
import io.github.panteliszara.warexis.inventory.reservations.application.OrderPlaced;
import io.github.panteliszara.warexis.inventory.reservations.domain.InventoryReservation;
import io.github.panteliszara.warexis.inventory.reservations.domain.InventoryReservation.Status;
import io.github.panteliszara.warexis.inventory.reservations.domain.InventoryReservation.ReasonCode;
import io.github.panteliszara.warexis.inventory.reservations.domain.ReservationLine;
import io.github.panteliszara.warexis.inventory.stock.application.StockItemService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(properties = "spring.datasource.hikari.connection-init-sql=SET statement_timeout = '10s'")
@Testcontainers
class InventoryReservationHardeningTests {
    @Container @ServiceConnection
    static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18.6");
    @Autowired private InventoryReservationService reservations;
    @Autowired private StockItemService stocks;
    @Autowired private JdbcTemplate jdbc;

    @RepeatedTest(5)
    void thirtySixSynchronizedOrdersNeverOversellTenUnits() throws Exception {
        var sku = stock("Stock", 10);
        var inputs = IntStream.range(0, 36).mapToObj(i -> input(line(sku, 1))).toList();
        var decisions = concurrent(inputs);
        assertThat(decisions.stream().filter(d -> d.status() == Status.RESERVED)).hasSize(10);
        assertThat(decisions.stream().filter(d -> d.status() == Status.REJECTED)).hasSize(26);
        assertThat(decisions.stream().filter(d -> d.status() == Status.REJECTED))
                .allMatch(d -> d.reasonCode() == ReasonCode.INSUFFICIENT_STOCK);
        assertThat(stocks.get(sku).getReserved()).isEqualTo(10);
        assertCommittedGraph(inputs, 36);
        assertStockMatchesCommittedLines(sku);
    }

    @RepeatedTest(5)
    void overlappingSkuCyclesAndOppositeInputOrdersRemainAtomicUnderContention() throws Exception {
        var suffix = UUID.randomUUID();
        var skus = List.of("A-" + suffix, "a-" + suffix, "Z-" + suffix, "z-" + suffix);
        skus.forEach(sku -> stocks.create(sku, 10));
        var inputs = IntStream.range(0, 30).mapToObj(i -> {
            var first = line(skus.get(i % skus.size()), 1);
            var second = line(skus.get((i + 1) % skus.size()), 1);
            return (i / skus.size()) % 2 == 0 ? input(first, second) : input(second, first);
        }).toList();
        var decisions = concurrent(inputs);
        assertThat(decisions).hasSize(30);
        assertThat(decisions).anyMatch(d -> d.status() == Status.RESERVED)
                .anyMatch(d -> d.status() == Status.REJECTED);
        assertCommittedGraph(inputs, 30);
        skus.forEach(this::assertStockMatchesCommittedLines);
        for (int i = 0; i < inputs.size(); i++) {
            assertThat(decisions.get(i).lines()).containsExactlyElementsOf(inputs.get(i).lines());
        }
    }

    @RepeatedTest(5)
    void twentyFourConcurrentCopiesAndDistinctEventAliasesProduceOneBusinessEffect() throws Exception {
        var sku = stock("Copies", 10);
        var original = input(line(sku, 2));
        var duplicates = java.util.Collections.nCopies(24, original);
        var first = concurrent(duplicates);
        assertThat(first).allMatch(first.getFirst()::equals);
        assertCommittedGraph(duplicates, 1);
        assertStockMatchesCommittedLines(sku);
        assertThat(stocks.get(sku).getReserved()).isEqualTo(2);

        var otherSku = stock("Aliases", 10);
        var order = input(line(otherSku, 3));
        var aliases = IntStream.range(0, 24)
                .mapToObj(i -> new OrderPlaced(UUID.randomUUID(), order.orderId(), order.lines())).toList();
        var decisions = concurrent(aliases);
        assertThat(decisions).allMatch(decisions.getFirst()::equals);
        assertCommittedGraph(aliases, 24);
        assertStockMatchesCommittedLines(otherSku);
        assertThat(stocks.get(otherSku).getReserved()).isEqualTo(3);
        var winner = decisions.getFirst();
        assertThat(aliases).extracting(OrderPlaced::eventId).contains(winner.sourceEventId());
        assertThat(jdbc.queryForObject("SELECT causation_id FROM outbox_events WHERE aggregate_id = ?", UUID.class,
                winner.reservationId())).isEqualTo(winner.sourceEventId());
    }

    @Test
    void concurrentRejectedRedeliveryRemainsOneDecisionAfterReplenishment() throws Exception {
        var sku = stock("Rejected", 0);
        var original = input(line(sku, 2));
        var rejected = reservations.process(original);
        assertThat(rejected.reasonCode()).isEqualTo(ReasonCode.INSUFFICIENT_STOCK);
        stocks.update(sku, 10);
        var copies = new ArrayList<OrderPlaced>(java.util.Collections.nCopies(12, original));
        IntStream.range(0, 12).forEach(i -> copies.add(new OrderPlaced(UUID.randomUUID(), original.orderId(), original.lines())));
        assertThat(concurrent(copies)).allMatch(rejected::equals);
        assertCommittedGraph(copies, 13);
        assertStockMatchesCommittedLines(sku);
        assertThat(stocks.get(sku).getReserved()).isZero();
        assertThat(jdbc.queryForObject("SELECT event_type FROM outbox_events WHERE correlation_id = ?", String.class,
                original.orderId())).isEqualTo("InventoryRejected");
    }

    enum Boundary {
        SECOND_STOCK_UPDATE("stock_items", "UPDATE"),
        RESERVATION_INSERT("inventory_reservations", "INSERT"),
        PROCESSED_EVENT_INSERT("processed_order_events", "INSERT"),
        OUTBOX_INSERT("outbox_events", "INSERT");
        final String table;
        final String operation;
        Boundary(String table, String operation) { this.table = table; this.operation = operation; }
    }

    @ParameterizedTest
    @EnumSource(Boundary.class)
    void faultAtEachPersistenceBoundaryRollsBackEverythingAndSameInputCanRecover(Boundary boundary) {
        var first = stock("FaultA", 8);
        var second = stock("FaultB", 8);
        var input = input(line(first, 3), line(second, 4));
        installFault(boundary);
        try {
            assertThatThrownBy(() -> reservations.process(input)).rootCause().isInstanceOf(SQLException.class)
                    .satisfies(cause -> {
                        var sql = (SQLException) cause;
                        assertThat(sql.getSQLState()).isEqualTo("P0001");
                        assertThat(sql.getMessage()).contains("test fault " + boundary.name());
                    });
        } finally { removeFault(boundary); }
        assertThat(stocks.get(first).getReserved()).isZero();
        assertThat(stocks.get(second).getReserved()).isZero();
        assertThat(stocks.get(first).getOnHand()).isEqualTo(8);
        assertThat(stocks.get(second).getOnHand()).isEqualTo(8);
        assertThat(count("SELECT count(*) FROM inventory_reservations WHERE order_id = ?", input.orderId())).isZero();
        assertThat(count("SELECT count(*) FROM processed_order_events WHERE event_id = ?", input.eventId())).isZero();
        assertThat(count("SELECT count(*) FROM outbox_events WHERE correlation_id = ?", input.orderId())).isZero();
        var recovered = reservations.process(input);
        assertThat(recovered.status()).isEqualTo(Status.RESERVED);
        assertThat(reservations.process(input)).isEqualTo(recovered);
        assertCommittedGraph(List.of(input), 1);
        assertStockMatchesCommittedLines(first);
        assertStockMatchesCommittedLines(second);
        assertThat(stocks.get(first).getReserved()).isEqualTo(3);
        assertThat(stocks.get(second).getReserved()).isEqualTo(4);
    }

    private void installFault(Boundary boundary) {
        // Transaction-local counter makes SECOND_STOCK_UPDATE fail only after a prior UPDATE ran.
        var body = boundary == Boundary.SECOND_STOCK_UPDATE ? """
                DECLARE updates integer;
                BEGIN
                    updates := COALESCE(NULLIF(current_setting('warexis.test_stock_updates', true), ''), '0')::integer + 1;
                    PERFORM set_config('warexis.test_stock_updates', updates::text, true);
                    IF updates = 2 THEN RAISE EXCEPTION 'test fault SECOND_STOCK_UPDATE'; END IF;
                    RETURN NEW;
                END
                """ : "BEGIN RAISE EXCEPTION 'test fault " + boundary.name() + "'; END";
        jdbc.execute("CREATE FUNCTION hardening_fault() RETURNS trigger LANGUAGE plpgsql AS $$ " + body + " $$");
        jdbc.execute("CREATE TRIGGER hardening_fault BEFORE " + boundary.operation + " ON " + boundary.table
                + " FOR EACH ROW EXECUTE FUNCTION hardening_fault()");
    }

    private void removeFault(Boundary boundary) {
        jdbc.execute("DROP TRIGGER hardening_fault ON " + boundary.table);
        jdbc.execute("DROP FUNCTION hardening_fault()");
    }

    private List<InventoryReservation> concurrent(List<OrderPlaced> inputs) throws Exception {
        var ready = new CountDownLatch(inputs.size());
        var start = new CountDownLatch(1);
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            var futures = inputs.stream().map(input -> executor.submit(() -> {
                ready.countDown();
                start.await();
                return reservations.process(input);
            })).toList();
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            var results = new ArrayList<InventoryReservation>();
            for (var future : futures) results.add(future.get(15, TimeUnit.SECONDS));
            return results;
        } finally {
            start.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(15, TimeUnit.SECONDS)).isTrue();
        }
    }

    private void assertCommittedGraph(List<OrderPlaced> inputs, int processedEvents) {
        var orderIds = inputs.stream().map(OrderPlaced::orderId).distinct().toList();
        int processed = 0;
        for (var orderId : orderIds) {
            assertThat(count("SELECT count(*) FROM inventory_reservations WHERE order_id = ?", orderId)).isEqualTo(1);
            assertThat(count("SELECT count(*) FROM outbox_events WHERE correlation_id = ?", orderId)).isEqualTo(1);
            processed += count("SELECT count(*) FROM processed_order_events p JOIN inventory_reservations r USING (reservation_id) WHERE r.order_id = ?", orderId);
            assertThat(count("""
                    SELECT count(*) FROM outbox_events o JOIN inventory_reservations r ON r.reservation_id = o.aggregate_id
                    WHERE r.order_id = ? AND o.causation_id = r.source_event_id
                    AND o.event_json -> 'payload' -> 'lines' = r.lines
                    AND o.event_type = CASE r.status WHEN 'RESERVED' THEN 'InventoryReserved' ELSE 'InventoryRejected' END
                    """, orderId)).isEqualTo(1);
        }
        assertThat(processed).isEqualTo(processedEvents);
        for (var eventId : inputs.stream().map(OrderPlaced::eventId).distinct().toList()) {
            assertThat(count("SELECT count(*) FROM processed_order_events WHERE event_id = ?", eventId)).isEqualTo(1);
        }
    }

    private void assertStockMatchesCommittedLines(String sku) {
        var reservedDemand = jdbc.queryForObject("""
                SELECT COALESCE(sum((line ->> 'quantity')::bigint), 0)
                FROM inventory_reservations r CROSS JOIN LATERAL jsonb_array_elements(r.lines) line
                WHERE r.status = 'RESERVED' AND line ->> 'sku' = ?
                """, Long.class, sku);
        var stock = stocks.get(sku);
        assertThat((long) stock.getReserved()).isEqualTo(reservedDemand);
        assertThat(stock.getReserved()).isBetween(0, stock.getOnHand());
    }

    private int count(String sql, Object value) { return jdbc.queryForObject(sql, Integer.class, value); }
    private String stock(String prefix, int amount) {
        var sku = prefix + "-" + UUID.randomUUID();
        stocks.create(sku, amount);
        return sku;
    }
    private ReservationLine line(String sku, int quantity) { return new ReservationLine(UUID.randomUUID(), sku, quantity); }
    private OrderPlaced input(ReservationLine... lines) { return new OrderPlaced(UUID.randomUUID(), UUID.randomUUID(), List.of(lines)); }
}
