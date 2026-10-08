package io.github.panteliszara.warexis.order;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.PlatformTransactionManager;
import io.github.panteliszara.warexis.order.messaging.KafkaConfiguration;
import io.github.panteliszara.warexis.order.messaging.MessagingProperties;
import io.github.panteliszara.warexis.order.messaging.OutboxRelay;
import io.github.panteliszara.warexis.order.orders.application.OrderService;
import io.github.panteliszara.warexis.order.orders.domain.OrderLineInput;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

@SpringBootTest(webEnvironment=SpringBootTest.WebEnvironment.RANDOM_PORT, properties="warexis.messaging.enabled=false")
@Import(OrderServiceApplicationTests.PostgresConfiguration.class)
class OutboxRelayReliabilityTests {
    @Autowired DataSource dataSource;
    @Autowired JdbcClient jdbc;
    @Autowired PlatformTransactionManager transactions;
    @Autowired OrderService orders;
    @Autowired io.github.panteliszara.warexis.order.messaging.InventoryOutcomeService outcomes;

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings={"OrderPlaced","OrderConfirmed"})
    @SuppressWarnings("unchecked")
    void competingRelayCannotOvertakeHeadWhileItsBrokerAcknowledgmentIsPending(String type) throws Exception {
        var first=orders.create(List.of(new OrderLineInput("First",1)));
        var second=orders.create(List.of(new OrderLineInput("Second",1)));
        if(type.equals("OrderConfirmed")) {
            for(var order:List.of(first,second)) {
                var origin=jdbc.sql("SELECT event_id FROM outbox_events WHERE aggregate_id=:id AND event_type='OrderPlaced'")
                        .param("id",order.id()).query(java.util.UUID.class).single();
                var reservation=java.util.UUID.randomUUID();
                outcomes.apply(new io.github.panteliszara.warexis.order.messaging.InventoryOutcome(java.util.UUID.randomUUID(),"InventoryReserved",
                        java.time.Instant.now(),reservation,order.id(),origin,order.id(),reservation,
                        order.lines().stream().map(line->new io.github.panteliszara.warexis.order.messaging.InventoryOutcome.Line(line.id(),line.sku(),line.quantity())).toList(),null));
            }
            jdbc.sql("UPDATE outbox_events SET published_at=GREATEST(CURRENT_TIMESTAMP,occurred_at) WHERE event_type='OrderPlaced'").update();
        }
        var kafka=(KafkaTemplate<byte[],byte[]>)mock(KafkaTemplate.class);
        var acknowledged=new CompletableFuture<SendResult<byte[],byte[]>>();
        var sent=new CountDownLatch(1);
        when(kafka.send(eq(KafkaConfiguration.ORDER_TOPIC),any(byte[].class),any(byte[].class))).thenAnswer(invocation->{
            sent.countDown();
            return acknowledged;
        });
        var properties=new MessagingProperties(false,Duration.ofMillis(10),Duration.ofMillis(50));
        var owner=new OutboxRelay(dataSource,jdbc,kafka,properties,transactions);
        var competitor=new OutboxRelay(dataSource,jdbc,kafka,properties,transactions);
        try(var executor=Executors.newSingleThreadExecutor()) {
            var task=executor.submit(owner::publishNext);
            try {
                assertThat(sent.await(5,TimeUnit.SECONDS)).isTrue();
                assertThat(jdbc.sql("SELECT published_at IS NULL AND publication_attempts=1 FROM outbox_events WHERE aggregate_id=:id AND event_type=:type")
                        .param("id",first.id()).param("type",type).query(Boolean.class).single()).isTrue();
                competitor.publishNext();
                verify(kafka,times(1)).send(eq(KafkaConfiguration.ORDER_TOPIC),any(byte[].class),any(byte[].class));
                assertThat(jdbc.sql("SELECT publication_attempts FROM outbox_events WHERE aggregate_id=:id AND event_type=:type")
                        .param("id",second.id()).param("type",type).query(Integer.class).single()).isZero();
            } finally {
                acknowledged.complete(null);
            }
            task.get(5,TimeUnit.SECONDS);
        }
        assertThat(jdbc.sql("SELECT published_at IS NOT NULL FROM outbox_events WHERE aggregate_id=:id AND event_type=:type")
                .param("id",first.id()).param("type",type).query(Boolean.class).single()).isTrue();
        competitor.publishNext();
        verify(kafka,times(2)).send(eq(KafkaConfiguration.ORDER_TOPIC),any(byte[].class),any(byte[].class));
        assertThat(jdbc.sql("SELECT published_at IS NOT NULL AND publication_attempts=1 FROM outbox_events WHERE aggregate_id=:id AND event_type=:type")
                .param("id",second.id()).param("type",type).query(Boolean.class).single()).isTrue();
    }
}
