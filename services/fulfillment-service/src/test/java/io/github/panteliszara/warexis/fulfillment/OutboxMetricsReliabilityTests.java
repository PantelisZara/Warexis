package io.github.panteliszara.warexis.fulfillment;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import javax.sql.DataSource;
import io.github.panteliszara.warexis.fulfillment.application.FulfillmentService;
import io.github.panteliszara.warexis.fulfillment.messaging.MessagingProperties;
import io.github.panteliszara.warexis.fulfillment.messaging.OutboxRelay;
import io.github.panteliszara.warexis.fulfillment.observability.OperationalMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

@SpringBootTest(webEnvironment=SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties={"WAREXIS_OAUTH2_ISSUER_URI=https://issuer.example.test/realms/warexis", "WAREXIS_OAUTH2_AUDIENCE=warexis-api",
            "warexis.metrics.test-isolation=true"})
@Import(FulfillmentServiceTests.PostgresConfiguration.class)
class OutboxMetricsReliabilityTests {
    @Autowired DataSource dataSource;
    @Autowired JdbcClient jdbc;
    @Autowired FulfillmentService service;
    @Autowired PlatformTransactionManager transactions;

    @BeforeEach void resetOwnedDatabase() {
        jdbc.sql("TRUNCATE outbox_events, processed_confirmations, fulfillment_lines, fulfillments CASCADE").update();
        jdbc.sql("UPDATE messaging_relay_control SET paused=false, failure_classification=NULL WHERE id=1").update();
    }

    @Test @SuppressWarnings("unchecked") void realRelayCountsFailedAttemptsRetriesAndOnlyDurablyPublishedSuccess() {
        var registry = new SimpleMeterRegistry();
        var kafka = (KafkaTemplate<byte[], byte[]>)mock(KafkaTemplate.class);
        when(kafka.send(any(org.apache.kafka.clients.producer.ProducerRecord.class))).thenReturn(
                CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")), CompletableFuture.completedFuture(null));
        var relay=relay(kafka,registry);
        var input=FulfillmentServiceTests.input(); service.accept(input); var plan=service.byOrder(input.orderId());
        service.ship(plan.id(),"METRICS-PUBLISH");
        relay.publishNext();
        assertThat(count(registry,"failed")).isEqualTo(1);
        assertThat(count(registry,"published")).isZero();
        assertThat(registry.get("warexis.outbox.retries").counter().count()).isZero();
        jdbc.sql("UPDATE outbox_events SET next_attempt_at=CURRENT_TIMESTAMP + INTERVAL '1 hour'").update();
        relay.publishNext();
        verify(kafka,times(1)).send(any(org.apache.kafka.clients.producer.ProducerRecord.class));
        jdbc.sql("UPDATE outbox_events SET next_attempt_at=NULL").update();
        relay.publishNext();
        assertThat(count(registry,"published")).isEqualTo(1);
        assertThat(registry.get("warexis.outbox.retries").counter().count()).isEqualTo(1);
        assertThat(jdbc.sql("SELECT publication_attempts FROM outbox_events").query(Integer.class).single()).isEqualTo(2);
        assertThat(jdbc.sql("SELECT published_at IS NOT NULL FROM outbox_events").query(Boolean.class).single()).isTrue();
        relay.publishNext();
        verify(kafka,times(2)).send(any(org.apache.kafka.clients.producer.ProducerRecord.class));
        assertThat(count(registry,"published")).isEqualTo(1);
    }

    @Test @SuppressWarnings("unchecked") void brokerAckWithRolledBackCheckpointCountsFailureRatherThanPublished() {
        var registry = new SimpleMeterRegistry();
        var kafka = (KafkaTemplate<byte[], byte[]>)mock(KafkaTemplate.class);
        when(kafka.send(any(org.apache.kafka.clients.producer.ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        var input=FulfillmentServiceTests.input(); service.accept(input); var plan=service.byOrder(input.orderId());
        service.ship(plan.id(),"METRICS-ROLLBACK");
        jdbc.sql("ALTER TABLE outbox_events ADD CONSTRAINT reject_metric_publication CHECK(published_at IS NULL) NOT VALID").update();
        try {
            relay(kafka,registry).publishNext();
            verify(kafka).send(any(org.apache.kafka.clients.producer.ProducerRecord.class));
            assertThat(count(registry,"published")).isZero();
            assertThat(count(registry,"failed")).isEqualTo(1);
            assertThat(jdbc.sql("SELECT published_at IS NULL FROM outbox_events").query(Boolean.class).single()).isTrue();
            assertThat(jdbc.sql("SELECT publication_attempts FROM outbox_events").query(Integer.class).single()).isEqualTo(1);
        } finally { jdbc.sql("ALTER TABLE outbox_events DROP CONSTRAINT reject_metric_publication").update(); }
    }

    private OutboxRelay relay(KafkaTemplate<byte[],byte[]> kafka,SimpleMeterRegistry registry) {
        return new OutboxRelay(dataSource,jdbc,kafka,new MessagingProperties(true,Duration.ofSeconds(1),Duration.ofMillis(50)),transactions,
                new OperationalMetrics(registry,true));
    }
    private double count(SimpleMeterRegistry registry,String result) {
        return registry.get("warexis.outbox.publications").tag("result",result).counter().count();
    }
}
