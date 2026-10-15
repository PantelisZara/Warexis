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

    @Test
    void terminalMetricsFollowAnAmbientTransactionCommitAndRemainZeroOnRollback() {
        var registry=new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
        var metrics=new io.github.panteliszara.warexis.order.observability.OperationalMetrics(registry,true);
        var transaction=new org.springframework.transaction.support.TransactionTemplate(transactions);
        transaction.executeWithoutResult(status->{
            metrics.published(); metrics.processed(true); metrics.dlt();
            assertThat(registry.get("warexis.outbox.publications").tag("result","published").counter().count()).isZero();
            status.setRollbackOnly();
        });
        assertThat(registry.get("warexis.outbox.publications").tag("result","published").counter().count()).isZero();
        assertThat(registry.get("warexis.messaging.deliveries").tag("result","processed").counter().count()).isZero();
        assertThat(registry.get("warexis.messaging.deliveries").tag("result","dlt").counter().count()).isZero();
        transaction.executeWithoutResult(status->{ metrics.published(); metrics.processed(true); metrics.dlt(); });
        assertThat(registry.get("warexis.outbox.publications").tag("result","published").counter().count()).isEqualTo(1);
        assertThat(registry.get("warexis.messaging.deliveries").tag("result","processed").counter().count()).isEqualTo(1);
        assertThat(registry.get("warexis.messaging.deliveries").tag("result","dlt").counter().count()).isEqualTo(1);
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings={"success","broker-failure","completion-failure"})
    @SuppressWarnings("unchecked")
    void publicationMetricsRequireBrokerAckAndCommittedCompletion(String mode) {
        jdbc.sql("UPDATE outbox_events SET published_at=GREATEST(CURRENT_TIMESTAMP,occurred_at) WHERE published_at IS NULL").update();
        var order=orders.create(List.of(new OrderLineInput("Metric",1)));
        var registry=new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
        var metrics=new io.github.panteliszara.warexis.order.observability.OperationalMetrics(registry,true);
        var kafka=(KafkaTemplate<byte[],byte[]>)mock(KafkaTemplate.class);
        when(kafka.send(eq(KafkaConfiguration.ORDER_TOPIC),any(byte[].class),any(byte[].class)))
                .thenReturn(mode.equals("broker-failure")?CompletableFuture.failedFuture(new IllegalStateException()):CompletableFuture.completedFuture(null));
        if(mode.equals("completion-failure")) {
            jdbc.sql("CREATE FUNCTION reject_metric_publication() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN IF NEW.published_at IS NOT NULL THEN RAISE EXCEPTION ''synthetic metadata failure''; END IF; RETURN NEW; END'").update();
            jdbc.sql("CREATE TRIGGER reject_metric_publication BEFORE UPDATE ON outbox_events FOR EACH ROW EXECUTE FUNCTION reject_metric_publication()").update();
        }
        try {
            // This is an actual second send attempt, not a wait/pause observation.
            jdbc.sql("UPDATE outbox_events SET publication_attempts=1 WHERE aggregate_id=:id").param("id",order.id()).update();
            new OutboxRelay(dataSource,jdbc,kafka,new MessagingProperties(false,Duration.ofMillis(10),Duration.ofMillis(50)),transactions,metrics).publishNext();
            assertThat(registry.get("warexis.outbox.publications").tag("result","published").counter().count()).isEqualTo(mode.equals("success")?1:0);
            assertThat(registry.get("warexis.outbox.publications").tag("result","failed").counter().count()).isEqualTo(mode.equals("success")?0:1);
            assertThat(registry.get("warexis.outbox.retries").counter().count()).isEqualTo(1);
            assertThat(jdbc.sql("SELECT published_at IS NOT NULL FROM outbox_events WHERE aggregate_id=:id").param("id",order.id()).query(Boolean.class).single()).isEqualTo(mode.equals("success"));
        } finally {
            if(mode.equals("completion-failure")) {
                jdbc.sql("DROP TRIGGER reject_metric_publication ON outbox_events").update();
                jdbc.sql("DROP FUNCTION reject_metric_publication()").update();
            }
            jdbc.sql("UPDATE outbox_events SET published_at=GREATEST(CURRENT_TIMESTAMP,occurred_at) WHERE published_at IS NULL").update();
        }
    }

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
