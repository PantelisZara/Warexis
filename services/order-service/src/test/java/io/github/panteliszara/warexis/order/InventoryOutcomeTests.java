package io.github.panteliszara.warexis.order;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;
import io.github.panteliszara.warexis.order.messaging.InventoryOutcome;
import io.github.panteliszara.warexis.order.messaging.InventoryOutcomeService;
import io.github.panteliszara.warexis.order.messaging.PoisonMessageException;
import io.github.panteliszara.warexis.order.orders.application.OrderService;
import io.github.panteliszara.warexis.order.orders.application.OrderView;
import io.github.panteliszara.warexis.order.orders.domain.OrderLineInput;
import io.github.panteliszara.warexis.order.orders.domain.OrderStatus;

import static org.assertj.core.api.Assertions.*;

@SpringBootTest(webEnvironment=SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(OrderServiceApplicationTests.PostgresConfiguration.class)
class InventoryOutcomeTests {
    @Autowired OrderService orders;
    @Autowired InventoryOutcomeService outcomes;
    @Autowired JdbcClient jdbc;
    @Autowired ObjectMapper mapper;
    @Autowired PlatformTransactionManager transactions;

    @Test
    void commitsConfirmedDecisionAndBothMessageAndBusinessIdempotency() {
        var order = create();
        var result = result(order, "InventoryReserved", null);
        outcomes.apply(result);
        var confirmation = confirmation(order);
        var terminal = orders.get(order.id());
        assertThat(terminal.status()).isEqualTo(OrderStatus.CONFIRMED);
        outcomes.apply(result);
        var alias = new InventoryOutcome(UUID.randomUUID(),result.eventType(),result.occurredAt(),result.aggregateId(),
                result.correlationId(),result.causationId(),result.orderId(),result.reservationId(),result.lines(),result.reasonCode());
        outcomes.apply(alias);
        assertThat(orders.get(order.id())).isEqualTo(terminal);
        assertThat(confirmation(order)).isEqualTo(confirmation);
        assertThat(confirmation.path("eventType").asString()).isEqualTo("OrderConfirmed");
        assertThat(confirmation.path("causationId").asString()).isEqualTo(result.eventId().toString());
        assertThat(confirmation.path("payload").path("reservationId").asString()).isEqualTo(result.reservationId().toString());
        assertThat(confirmation.path("payload").path("lines")).isEqualTo(mapper.readTree(
                jdbc.sql("SELECT event_json::text FROM outbox_events WHERE aggregate_id=:id AND event_type='OrderPlaced'")
                        .param("id",order.id()).query(String.class).single()).path("payload").path("lines"));
        assertThat(jdbc.sql("SELECT causation_id FROM outbox_events WHERE aggregate_id=:id AND event_type='OrderConfirmed'")
                .param("id",order.id()).query(UUID.class).single()).isEqualTo(result.eventId());
        assertThatThrownBy(()->jdbc.sql("UPDATE outbox_events SET causation_id=:cause WHERE aggregate_id=:id AND event_type='OrderConfirmed'")
                .param("cause",UUID.randomUUID()).param("id",order.id()).update()).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
        assertThat(confirmation(order)).isEqualTo(confirmation);
        assertThat(jdbc.sql("SELECT count(*) FROM outbox_events WHERE aggregate_id=:id AND event_type='OrderConfirmed'")
                .param("id",order.id()).query(Long.class).single()).isEqualTo(1);
        assertThat(jdbc.sql("SELECT count(*) FROM inventory_decisions WHERE order_id=:id").param("id",order.id()).query(Long.class).single()).isEqualTo(1);
        assertThat(jdbc.sql("SELECT count(*) FROM processed_inventory_events WHERE order_id=:id").param("id",order.id()).query(Long.class).single()).isEqualTo(2);
    }

    @Test
    void rejectsBusinessOutcomeWithoutTechnicalFailure() {
        var order=create();
        var rejected=result(order,"InventoryRejected","INSUFFICIENT_STOCK");
        outcomes.apply(rejected);
        var terminal=orders.get(order.id());
        outcomes.apply(rejected);
        outcomes.apply(new InventoryOutcome(UUID.randomUUID(),rejected.eventType(),rejected.occurredAt(),rejected.aggregateId(),
                rejected.correlationId(),rejected.causationId(),rejected.orderId(),rejected.reservationId(),rejected.lines(),rejected.reasonCode()));
        assertThat(terminal.status()).isEqualTo(OrderStatus.REJECTED);
        assertThat(jdbc.sql("SELECT count(*) FROM outbox_events WHERE aggregate_id=:id AND event_type='OrderConfirmed'")
                .param("id",order.id()).query(Long.class).single()).isZero();
        assertThat(orders.get(order.id())).isEqualTo(terminal);
        assertThat(jdbc.sql("SELECT count(*) FROM inventory_decisions WHERE order_id=:id").param("id",order.id()).query(Long.class).single()).isEqualTo(1);
        assertThat(jdbc.sql("SELECT count(*) FROM processed_inventory_events WHERE order_id=:id").param("id",order.id()).query(Long.class).single()).isEqualTo(2);
    }

    @Test
    void conflictsNeverOverwriteTerminalDecisionOrRecordNewEvent() {
        var order=create();
        var original=result(order,"InventoryReserved",null);
        outcomes.apply(original);
        var savedConfirmation=confirmation(order);
        var conflict=new InventoryOutcome(UUID.randomUUID(),"InventoryRejected",original.occurredAt(),original.aggregateId(),
                original.correlationId(),original.causationId(),original.orderId(),original.reservationId(),original.lines(),"UNKNOWN_SKU");
        assertThatThrownBy(()->outcomes.apply(conflict)).isInstanceOf(PoisonMessageException.class);
        assertThat(orders.get(order.id()).status()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(jdbc.sql("SELECT count(*) FROM processed_inventory_events WHERE event_id=:id").param("id",conflict.eventId()).query(Long.class).single()).isZero();
        var reusedId=new InventoryOutcome(original.eventId(),original.eventType(),original.occurredAt().plusSeconds(1),original.aggregateId(),
                original.correlationId(),original.causationId(),original.orderId(),original.reservationId(),original.lines(),null);
        assertThatThrownBy(()->outcomes.apply(reusedId)).isInstanceOf(PoisonMessageException.class);
        assertThat(confirmation(order)).isEqualTo(savedConfirmation);
    }

    @Test
    void wrongCausationAndReorderedOrChangedOriginalLinesCannotResolveOrder() {
        var order=create();
        var result=result(order,"InventoryReserved",null);
        var wrong=new InventoryOutcome(result.eventId(),result.eventType(),result.occurredAt(),result.aggregateId(),
                result.correlationId(),UUID.randomUUID(),result.orderId(),result.reservationId(),result.lines(),null);
        assertThatThrownBy(()->outcomes.apply(wrong)).isInstanceOf(PoisonMessageException.class);
        var reordered=new InventoryOutcome(UUID.randomUUID(),result.eventType(),result.occurredAt(),result.aggregateId(),
                result.correlationId(),result.causationId(),result.orderId(),result.reservationId(),List.of(result.lines().get(1),result.lines().get(0)),null);
        assertThatThrownBy(()->outcomes.apply(reordered)).isInstanceOf(PoisonMessageException.class);
        assertThat(orders.get(order.id()).status()).isEqualTo(OrderStatus.PENDING_INVENTORY);
    }

    @Test
    void internalInputRejectsInvalidTypeReasonPairingAndDuplicateLines() {
        var result=result(create(),"InventoryReserved",null);
        assertThatThrownBy(()->new InventoryOutcome(result.eventId(),"Invalid",result.occurredAt(),result.aggregateId(),
                result.correlationId(),result.causationId(),result.orderId(),result.reservationId(),result.lines(),null)).isInstanceOf(PoisonMessageException.class);
        assertThatThrownBy(()->new InventoryOutcome(result.eventId(),"InventoryRejected",result.occurredAt(),result.aggregateId(),
                result.correlationId(),result.causationId(),result.orderId(),result.reservationId(),result.lines(),null)).isInstanceOf(PoisonMessageException.class);
        assertThatThrownBy(()->new InventoryOutcome(result.eventId(),"InventoryReserved",result.occurredAt(),result.aggregateId(),
                result.correlationId(),result.causationId(),result.orderId(),result.reservationId(),List.of(result.lines().getFirst(),result.lines().getFirst()),null)).isInstanceOf(PoisonMessageException.class);
    }

    @Test
    void rollbackAfterDecisionWritesLeavesPendingOrderAndNoProcessedEvent() {
        var order=create();
        var result=result(order,"InventoryReserved",null);
        assertThatThrownBy(()->new TransactionTemplate(transactions).execute(status->{
            outcomes.apply(result);
            throw new IllegalStateException("rollback");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(orders.get(order.id()).status()).isEqualTo(OrderStatus.PENDING_INVENTORY);
        assertThat(jdbc.sql("SELECT count(*) FROM inventory_decisions WHERE order_id=:id").param("id",order.id()).query(Long.class).single()).isZero();
        assertThat(jdbc.sql("SELECT count(*) FROM processed_inventory_events WHERE event_id=:id").param("id",result.eventId()).query(Long.class).single()).isZero();
    }

    @Test
    void confirmationOutboxFailureRollsBackStatusDecisionAndInputLedgerThenSameInputRecovers() {
        var order=create();
        var result=result(order,"InventoryReserved",null);
        jdbc.sql("ALTER TABLE outbox_events ADD CONSTRAINT reject_confirmed CHECK(event_type<>'OrderConfirmed') NOT VALID").update();
        try {
            assertThatThrownBy(()->outcomes.apply(result)).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
            assertThat(orders.get(order.id())).isEqualTo(order);
            assertThat(jdbc.sql("SELECT count(*) FROM inventory_decisions WHERE order_id=:id").param("id",order.id()).query(Long.class).single()).isZero();
            assertThat(jdbc.sql("SELECT count(*) FROM processed_inventory_events WHERE order_id=:id").param("id",order.id()).query(Long.class).single()).isZero();
            assertThat(jdbc.sql("SELECT count(*) FROM outbox_events WHERE aggregate_id=:id").param("id",order.id()).query(Long.class).single()).isEqualTo(1);
        } finally {
            jdbc.sql("ALTER TABLE outbox_events DROP CONSTRAINT reject_confirmed").update();
        }
        outcomes.apply(result);
        assertThat(orders.get(order.id()).status()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(confirmation(order).path("causationId").asString()).isEqualTo(result.eventId().toString());
    }

    @Test
    void suppressedConfirmationInsertRollsBackAllLocalEffectsThenSameInputRecovers() {
        var order=create();
        var result=result(order,"InventoryReserved",null);
        jdbc.sql("""
                CREATE FUNCTION suppress_confirmation_insert() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    IF NEW.event_type='OrderConfirmed' THEN RETURN NULL; END IF;
                    RETURN NEW;
                END $$
                """).update();
        jdbc.sql("CREATE TRIGGER suppress_confirmation_insert BEFORE INSERT ON outbox_events FOR EACH ROW EXECUTE FUNCTION suppress_confirmation_insert()").update();
        try {
            assertThatThrownBy(()->outcomes.apply(result)).isInstanceOf(org.springframework.dao.InvalidDataAccessApiUsageException.class)
                    .hasRootCauseInstanceOf(IllegalStateException.class)
                    .hasMessage("Outbox append did not persist one event.");
            assertThat(orders.get(order.id())).isEqualTo(order);
            assertThat(jdbc.sql("SELECT count(*) FROM inventory_decisions WHERE order_id=:id").param("id",order.id()).query(Long.class).single()).isZero();
            assertThat(jdbc.sql("SELECT count(*) FROM processed_inventory_events WHERE order_id=:id").param("id",order.id()).query(Long.class).single()).isZero();
            assertThat(jdbc.sql("SELECT count(*) FROM outbox_events WHERE aggregate_id=:id").param("id",order.id()).query(Long.class).single()).isEqualTo(1);
        } finally {
            jdbc.sql("DROP TRIGGER suppress_confirmation_insert ON outbox_events").update();
            jdbc.sql("DROP FUNCTION suppress_confirmation_insert()").update();
        }
        outcomes.apply(result);
        assertThat(orders.get(order.id()).status()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(confirmation(order).path("causationId").asString()).isEqualTo(result.eventId().toString());
        assertThat(jdbc.sql("SELECT count(*) FROM processed_inventory_events WHERE event_id=:id")
                .param("id",result.eventId()).query(Long.class).single()).isEqualTo(1);
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings={"inventory_decisions","processed_inventory_events"})
    void suppressedDecisionOrInputLedgerInsertRollsBackConfirmationAndRecovers(String table) {
        var order=create();
        var result=result(order,"InventoryReserved",null);
        jdbc.sql("CREATE FUNCTION suppress_outcome_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$").update();
        jdbc.sql("CREATE TRIGGER suppress_outcome_insert BEFORE INSERT ON "+table+" FOR EACH ROW EXECUTE FUNCTION suppress_outcome_insert()").update();
        try {
            assertThatThrownBy(()->outcomes.apply(result)).isInstanceOf(IllegalStateException.class)
                    .hasMessage("Inventory outcome write did not persist one row.");
            assertThat(orders.get(order.id())).isEqualTo(order);
            assertThat(jdbc.sql("SELECT count(*) FROM inventory_decisions WHERE order_id=:id").param("id",order.id()).query(Long.class).single()).isZero();
            assertThat(jdbc.sql("SELECT count(*) FROM processed_inventory_events WHERE order_id=:id").param("id",order.id()).query(Long.class).single()).isZero();
            assertThat(jdbc.sql("SELECT count(*) FROM outbox_events WHERE aggregate_id=:id").param("id",order.id()).query(Long.class).single()).isEqualTo(1);
        } finally {
            jdbc.sql("DROP TRIGGER suppress_outcome_insert ON "+table).update();
            jdbc.sql("DROP FUNCTION suppress_outcome_insert()").update();
        }
        outcomes.apply(result);
        var originalConfirmation=confirmation(order);
        assertThat(orders.get(order.id()).status()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(originalConfirmation.path("causationId").asString()).isEqualTo(result.eventId().toString());
        outcomes.apply(result);
        assertThat(confirmation(order)).isEqualTo(originalConfirmation);
        assertThat(jdbc.sql("SELECT count(*) FROM processed_inventory_events WHERE event_id=:id")
                .param("id",result.eventId()).query(Long.class).single()).isEqualTo(1);
    }

    @Test
    void concurrentIdenticalAndAliasedOutcomesEmitOneConfirmationWithTheWinningOrigin() throws Exception {
        var order=create();
        var first=result(order,"InventoryReserved",null);
        var alias=new InventoryOutcome(UUID.randomUUID(),first.eventType(),first.occurredAt(),first.aggregateId(),
                first.correlationId(),first.causationId(),first.orderId(),first.reservationId(),first.lines(),null);
        var ready=new CountDownLatch(8);
        var go=new CountDownLatch(1);
        try(var executor=Executors.newFixedThreadPool(8)) {
            var tasks=java.util.stream.IntStream.range(0,8).mapToObj(i->executor.submit(()->{
                ready.countDown();
                if(!go.await(10,TimeUnit.SECONDS)) throw new AssertionError("Concurrent start timed out");
                outcomes.apply(i%2==0?first:alias);
                return true;
            })).toList();
            try { assertThat(ready.await(10,TimeUnit.SECONDS)).isTrue(); }
            finally { go.countDown(); }
            for(var task:tasks) assertThat(task.get(20,TimeUnit.SECONDS)).isTrue();
        }
        assertThat(orders.get(order.id()).status()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(confirmation(order).path("causationId").asString()).isIn(first.eventId().toString(),alias.eventId().toString());
        assertThat(jdbc.sql("SELECT count(*) FROM outbox_events WHERE aggregate_id=:id AND event_type='OrderConfirmed'")
                .param("id",order.id()).query(Long.class).single()).isEqualTo(1);
        assertThat(jdbc.sql("SELECT count(*) FROM processed_inventory_events WHERE order_id=:id")
                .param("id",order.id()).query(Long.class).single()).isEqualTo(2);
    }

    private tools.jackson.databind.JsonNode confirmation(OrderView order) {
        return mapper.readTree(jdbc.sql("SELECT event_json::text FROM outbox_events WHERE aggregate_id=:id AND event_type='OrderConfirmed'")
                .param("id",order.id()).query(String.class).single());
    }

    private OrderView create() {
        return orders.create(List.of(new OrderLineInput("Abc",2),new OrderLineInput("Abc",3)));
    }

    private InventoryOutcome result(OrderView order,String type,String reason) {
        var reservation=UUID.randomUUID();
        var causation=jdbc.sql("SELECT event_id FROM outbox_events WHERE aggregate_id=:id").param("id",order.id()).query(UUID.class).single();
        return new InventoryOutcome(UUID.randomUUID(),type,Instant.now(),reservation,order.id(),causation,order.id(),reservation,
                order.lines().stream().map(line->new InventoryOutcome.Line(line.id(),line.sku(),line.quantity())).toList(),reason);
    }
}
