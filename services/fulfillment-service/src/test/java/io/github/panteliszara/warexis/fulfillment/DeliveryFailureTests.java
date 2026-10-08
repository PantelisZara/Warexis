package io.github.panteliszara.warexis.fulfillment;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import io.github.panteliszara.warexis.fulfillment.messaging.DeliveryState;
import io.github.panteliszara.warexis.fulfillment.messaging.ConfirmationListener;
import io.github.panteliszara.warexis.fulfillment.application.FulfillmentService;
import io.github.panteliszara.warexis.fulfillment.messaging.KafkaConfiguration;
import io.github.panteliszara.warexis.fulfillment.messaging.MessagingProperties;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.any;

@SpringBootTest(webEnvironment=SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import({FulfillmentServiceTests.PostgresConfiguration.class,DeliveryFailureTests.StateConfiguration.class})
class DeliveryFailureTests {
    @Autowired DeliveryState deliveries;
    @Autowired JdbcClient jdbc;
    @Autowired FulfillmentService outcomes;

    @Test
    @SuppressWarnings("unchecked")
    void failedDltBudgetIsDurableAndReplacementListenerCannotTryAgainOrAcknowledge() {
        var kafka=(KafkaTemplate<byte[],byte[]>)mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.failedFuture(new KafkaException("synthetic broker failure")));
        var registry=mock(KafkaListenerEndpointRegistry.class);
        var ack=mock(Acknowledgment.class);
        var consumer=(Consumer<byte[],byte[]>)mock(Consumer.class);
        var record=new ConsumerRecord<byte[],byte[]>(KafkaConfiguration.ORDER_TOPIC,0,900000000L,
                UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8),"{".getBytes(StandardCharsets.UTF_8));
        new ConfirmationListener(outcomes,deliveries,kafka,registry).receive(record,ack,consumer);
        var state=deliveries.load(record);
        assertThat(state.phase()).isEqualTo("PAUSED");
        assertThat(state.processingAttempts()).isEqualTo(1);
        assertThat(state.dltAttempts()).isEqualTo(4);
        verify(kafka,times(4)).send(any(ProducerRecord.class));
        verify(consumer).seek(new TopicPartition(record.topic(),record.partition()),record.offset());
        verifyNoInteractions(ack);
        new ConfirmationListener(outcomes,deliveries,kafka,registry).receive(record,ack,consumer);
        verify(kafka,times(4)).send(any(ProducerRecord.class));
        verifyNoInteractions(ack);
    }

    @Test
    @SuppressWarnings("unchecked")
    void retryMetadataFailureFailsClosedWithoutBusinessOrBrokerAttempt() {
        var offset=900000001L;
        jdbc.sql("ALTER TABLE messaging_deliveries ADD CONSTRAINT reject_delivery_metadata CHECK(source_offset<>900000001) NOT VALID").update();
        try {
            var kafka=(KafkaTemplate<byte[],byte[]>)mock(KafkaTemplate.class);
            var business=mock(FulfillmentService.class);
            var ack=mock(Acknowledgment.class);
            var consumer=(Consumer<byte[],byte[]>)mock(Consumer.class);
            var record=new ConsumerRecord<byte[],byte[]>(KafkaConfiguration.ORDER_TOPIC,1,offset,
                    new byte[]{1},new byte[]{2});
            new ConfirmationListener(business,deliveries,kafka,mock(KafkaListenerEndpointRegistry.class)).receive(record,ack,consumer);
            verifyNoInteractions(kafka,business,ack);
            verify(consumer).seek(new TopicPartition(record.topic(),record.partition()),record.offset());
            assertThat(jdbc.sql("SELECT count(*) FROM messaging_deliveries WHERE source_offset=:offset").param("offset",offset).query(Long.class).single()).isZero();
        } finally {
            jdbc.sql("ALTER TABLE messaging_deliveries DROP CONSTRAINT reject_delivery_metadata").update();
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans={false,true})
    @SuppressWarnings("unchecked")
    void completionMetadataFailureAfterBusinessSuccessPausesWithoutAnotherAttemptOrAck(boolean suppressUpdate) {
        rejectCompletion(suppressUpdate);
        try {
            var kafka=(KafkaTemplate<byte[],byte[]>)mock(KafkaTemplate.class);
            var business=mock(FulfillmentService.class);
            var ack=mock(Acknowledgment.class);
            var consumer=(Consumer<byte[],byte[]>)mock(Consumer.class);
            var orderId=UUID.randomUUID();
            var reservationId=UUID.randomUUID();
            var json="""
                    {"eventId":"%s","eventType":"OrderConfirmed","eventVersion":1,
                    "occurredAt":"2026-10-07T00:00:00Z","aggregateId":"%s","correlationId":"%s",
                    "causationId":"%s","payload":{"orderId":"%s","reservationId":"%s",
                    "lines":[{"orderLineId":"%s","sku":"A","quantity":1}]}}
                    """.formatted(UUID.randomUUID(),orderId,orderId,UUID.randomUUID(),orderId,reservationId,UUID.randomUUID());
            var record=new ConsumerRecord<byte[],byte[]>(KafkaConfiguration.ORDER_TOPIC,2,suppressUpdate?900000102L:900000002L,
                    orderId.toString().getBytes(StandardCharsets.UTF_8),json.getBytes(StandardCharsets.UTF_8));
            new ConfirmationListener(business,deliveries,kafka,mock(KafkaListenerEndpointRegistry.class)).receive(record,ack,consumer);
            verify(business,times(1)).accept(any());
            verifyNoInteractions(kafka,ack);
            verify(consumer).seek(new TopicPartition(record.topic(),record.partition()),record.offset());
            assertThat(deliveries.load(record).phase()).isEqualTo("PROCESS");
            assertThat(deliveries.load(record).processingAttempts()).isEqualTo(1);
        } finally {
            restoreCompletion(suppressUpdate);
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans={false,true})
    @SuppressWarnings("unchecked")
    void completionMetadataFailureAfterDltAckPausesWithoutAnotherSendOrSourceAck(boolean suppressUpdate) {
        rejectCompletion(suppressUpdate);
        try {
            var kafka=(KafkaTemplate<byte[],byte[]>)mock(KafkaTemplate.class);
            when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
            var ack=mock(Acknowledgment.class);
            var consumer=(Consumer<byte[],byte[]>)mock(Consumer.class);
            var record=new ConsumerRecord<byte[],byte[]>(KafkaConfiguration.ORDER_TOPIC,2,suppressUpdate?900000103L:900000003L,
                    UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8),new byte[]{'{' });
            new ConfirmationListener(outcomes,deliveries,kafka,mock(KafkaListenerEndpointRegistry.class)).receive(record,ack,consumer);
            verify(kafka,times(1)).send(any(ProducerRecord.class));
            verifyNoInteractions(ack);
            verify(consumer).seek(new TopicPartition(record.topic(),record.partition()),record.offset());
            assertThat(deliveries.load(record).phase()).isEqualTo("DLT");
            assertThat(deliveries.load(record).dltAttempts()).isEqualTo(1);
        } finally {
            restoreCompletion(suppressUpdate);
        }
    }

    @Test
    void completedDeliveryCannotBeDowngradedToRuntimePauseByStaleOwner() {
        var record=new ConsumerRecord<byte[],byte[]>(KafkaConfiguration.ORDER_TOPIC,2,900000004L,new byte[0],new byte[0]);
        deliveries.load(record);
        deliveries.complete(record);
        assertThat(deliveries.pause(record)).isFalse();
        assertThat(deliveries.load(record).phase()).isEqualTo("COMPLETE");
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings={"PROCESS_0","PROCESS_4","DLT_4"})
    @SuppressWarnings("unchecked")
    void suppressedDurableAttemptTransitionOrPauseFailsClosedWithoutSpinning(String phase) {
        long offset=900000010L+java.util.Arrays.asList("PROCESS_0","PROCESS_4","DLT_4").indexOf(phase);
        var record=new ConsumerRecord<byte[],byte[]>(KafkaConfiguration.ORDER_TOPIC,1,offset,new byte[]{1},new byte[]{2});
        deliveries.load(record);
        if(phase.equals("PROCESS_4")) {
            jdbc.sql("UPDATE messaging_deliveries SET processing_attempts=4 WHERE source_offset=:offset").param("offset",offset).update();
        } else if(phase.equals("DLT_4")) {
            jdbc.sql("UPDATE messaging_deliveries SET phase='DLT',dlt_attempts=4 WHERE source_offset=:offset").param("offset",offset).update();
        }
        jdbc.sql("CREATE FUNCTION suppress_delivery_update() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NULL; END'").update();
        jdbc.sql("CREATE TRIGGER suppress_delivery_update BEFORE UPDATE ON messaging_deliveries FOR EACH ROW EXECUTE FUNCTION suppress_delivery_update()").update();
        try {
            var kafka=(KafkaTemplate<byte[],byte[]>)mock(KafkaTemplate.class);
            var business=mock(FulfillmentService.class);
            var ack=mock(Acknowledgment.class);
            var consumer=(Consumer<byte[],byte[]>)mock(Consumer.class);
            var listener=new ConfirmationListener(business,deliveries,kafka,mock(KafkaListenerEndpointRegistry.class));
            org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(Duration.ofSeconds(3),()->listener.receive(record,ack,consumer));
            verifyNoInteractions(kafka,business,ack);
            verify(consumer).seek(new TopicPartition(record.topic(),record.partition()),record.offset());
            var state=deliveries.load(record);
            assertThat(state.phase()).isEqualTo(phase.equals("DLT_4")?"DLT":"PROCESS");
            assertThat(state.processingAttempts()).isEqualTo(phase.equals("PROCESS_4")?4:0);
            assertThat(state.dltAttempts()).isEqualTo(phase.equals("DLT_4")?4:0);
        } finally {
            jdbc.sql("DROP TRIGGER suppress_delivery_update ON messaging_deliveries").update();
            jdbc.sql("DROP FUNCTION suppress_delivery_update()").update();
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans={false,true})
    @org.junit.jupiter.api.extension.ExtendWith(org.springframework.boot.test.system.OutputCaptureExtension.class)
    @SuppressWarnings("unchecked")
    void acknowledgmentFailureAfterCompleteLogsThePersistedBusinessOrDltAttempt(boolean quarantined,
            org.springframework.boot.test.system.CapturedOutput output) {
        var record=new ConsumerRecord<byte[],byte[]>(KafkaConfiguration.ORDER_TOPIC,1,
                quarantined?900000021L:900000020L,new byte[]{1},new byte[]{2});
        deliveries.load(record);
        int processingAttempts=quarantined?4:3;
        int dltAttempts=quarantined?2:0;
        jdbc.sql("UPDATE messaging_deliveries SET phase='COMPLETE',processing_attempts=:processing,dlt_attempts=:dlt WHERE source_offset=:offset")
                .param("processing",processingAttempts).param("dlt",dltAttempts).param("offset",record.offset()).update();
        var kafka=(KafkaTemplate<byte[],byte[]>)mock(KafkaTemplate.class);
        var business=mock(FulfillmentService.class);
        var ack=mock(Acknowledgment.class);
        var unsafe="UNSAFE-ACK-EXCEPTION-DETAIL";
        doThrow(new KafkaException(unsafe)).when(ack).acknowledge();
        var consumer=(Consumer<byte[],byte[]>)mock(Consumer.class);
        new ConfirmationListener(business,deliveries,kafka,mock(KafkaListenerEndpointRegistry.class))
                .receive(record,ack,consumer);
        verify(ack).acknowledge();
        verifyNoInteractions(business,kafka);
        verify(consumer).seek(new TopicPartition(record.topic(),record.partition()),record.offset());
        var persisted=deliveries.load(record);
        assertThat(persisted.phase()).isEqualTo("COMPLETE");
        assertThat(persisted.processingAttempts()).isEqualTo(processingAttempts);
        assertThat(persisted.dltAttempts()).isEqualTo(dltAttempts);
        int expectedAttempt=quarantined?dltAttempts:processingAttempts;
        assertThat(output.getAll()).contains("state=PAUSED, attempt="+expectedAttempt+", partition=1, offset="+record.offset())
                .doesNotContain(unsafe);
    }

    @Test
    @SuppressWarnings("unchecked")
    void validUnsubscribedCompletionMetadataFailurePausesWithoutSpendingBusinessAttempts() {
        var in=FulfillmentServiceTests.input();
        var root=(tools.jackson.databind.node.ObjectNode)tools.jackson.databind.json.JsonMapper.builder().build().readTree(EventJsonTests.envelope(in));
        root.put("eventType","OrderPlaced");root.remove("causationId");
        ((tools.jackson.databind.node.ObjectNode)root.path("payload")).remove("reservationId");
        var record=new ConsumerRecord<byte[],byte[]>(KafkaConfiguration.ORDER_TOPIC,0,900000030L,
                EventJsonTests.bytes(in.orderId().toString()),EventJsonTests.bytes(root.toString()));
        rejectCompletion(false);
        try {
            var business=mock(FulfillmentService.class);var kafka=(KafkaTemplate<byte[],byte[]>)mock(KafkaTemplate.class);
            var ack=mock(Acknowledgment.class);var consumer=(Consumer<byte[],byte[]>)mock(Consumer.class);
            new ConfirmationListener(business,deliveries,kafka,mock(KafkaListenerEndpointRegistry.class)).receive(record,ack,consumer);
            verifyNoInteractions(business,kafka,ack);
            verify(consumer).seek(new TopicPartition(record.topic(),record.partition()),record.offset());
            assertThat(deliveries.load(record).processingAttempts()).isZero();
            assertThat(deliveries.load(record).phase()).isEqualTo("PROCESS");
        } finally { restoreCompletion(false); }
    }

    private void rejectCompletion(boolean suppressUpdate) {
        if (suppressUpdate) {
            jdbc.sql("CREATE FUNCTION suppress_completion() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN IF NEW.phase=''COMPLETE'' THEN RETURN NULL; END IF; RETURN NEW; END'").update();
            jdbc.sql("CREATE TRIGGER suppress_completion BEFORE UPDATE ON messaging_deliveries FOR EACH ROW EXECUTE FUNCTION suppress_completion()").update();
        } else {
            jdbc.sql("ALTER TABLE messaging_deliveries ADD CONSTRAINT reject_completed_checkpoint CHECK(phase<>'COMPLETE') NOT VALID").update();
        }
    }

    private void restoreCompletion(boolean suppressUpdate) {
        if (suppressUpdate) {
            jdbc.sql("DROP TRIGGER suppress_completion ON messaging_deliveries").update();
            jdbc.sql("DROP FUNCTION suppress_completion()").update();
        } else {
            jdbc.sql("ALTER TABLE messaging_deliveries DROP CONSTRAINT reject_completed_checkpoint").update();
        }
    }

    @TestConfiguration(proxyBeanMethods=false)
    static class StateConfiguration {
        @Bean DeliveryState deliveryState(JdbcClient jdbc) {
            return new DeliveryState(jdbc,new MessagingProperties(true,Duration.ofMillis(10),Duration.ofMillis(50)));
        }
    }
}
