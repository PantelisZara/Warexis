package io.github.panteliszara.warexis.order;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
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
        var terminal = orders.get(order.id());
        assertThat(terminal.status()).isEqualTo(OrderStatus.CONFIRMED);
        outcomes.apply(result);
        var alias = new InventoryOutcome(UUID.randomUUID(),result.eventType(),result.occurredAt(),result.aggregateId(),
                result.correlationId(),result.causationId(),result.orderId(),result.reservationId(),result.lines(),result.reasonCode());
        outcomes.apply(alias);
        assertThat(orders.get(order.id())).isEqualTo(terminal);
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
        assertThat(orders.get(order.id())).isEqualTo(terminal);
        assertThat(jdbc.sql("SELECT count(*) FROM inventory_decisions WHERE order_id=:id").param("id",order.id()).query(Long.class).single()).isEqualTo(1);
        assertThat(jdbc.sql("SELECT count(*) FROM processed_inventory_events WHERE order_id=:id").param("id",order.id()).query(Long.class).single()).isEqualTo(2);
    }

    @Test
    void conflictsNeverOverwriteTerminalDecisionOrRecordNewEvent() {
        var order=create();
        var original=result(order,"InventoryReserved",null);
        outcomes.apply(original);
        var conflict=new InventoryOutcome(UUID.randomUUID(),"InventoryRejected",original.occurredAt(),original.aggregateId(),
                original.correlationId(),original.causationId(),original.orderId(),original.reservationId(),original.lines(),"UNKNOWN_SKU");
        assertThatThrownBy(()->outcomes.apply(conflict)).isInstanceOf(PoisonMessageException.class);
        assertThat(orders.get(order.id()).status()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(jdbc.sql("SELECT count(*) FROM processed_inventory_events WHERE event_id=:id").param("id",conflict.eventId()).query(Long.class).single()).isZero();
        var reusedId=new InventoryOutcome(original.eventId(),original.eventType(),original.occurredAt().plusSeconds(1),original.aggregateId(),
                original.correlationId(),original.causationId(),original.orderId(),original.reservationId(),original.lines(),null);
        assertThatThrownBy(()->outcomes.apply(reusedId)).isInstanceOf(PoisonMessageException.class);
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
