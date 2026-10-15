package io.github.panteliszara.warexis.fulfillment.observability;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.github.panteliszara.warexis.fulfillment.messaging.*;
import io.github.panteliszara.warexis.fulfillment.application.FulfillmentService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class MessagingMetricsTests {
    @Test void processedAndKnownBusinessReplayAreCountedOnlyAfterCompletion() {
        for (boolean newDecision : new boolean[]{true, false}) {
            var meters = new SimpleMeterRegistry();
            var outcomes = mock(FulfillmentService.class);
            when(outcomes.accept(any())).thenReturn(newDecision);
            var deliveries = mock(DeliveryState.class);
            var source = validRecord();
            when(deliveries.load(source)).thenReturn(state("PROCESS", 0, 0), state("COMPLETE", 1, 0));
            when(deliveries.begin(eq(source), any())).thenReturn(true);
            var ack = mock(Acknowledgment.class);
            listener(outcomes, deliveries, mock(KafkaTemplate.class), meters).receive(source, ack, mock(Consumer.class));
            assertThat(count(meters, "warexis.messaging.deliveries", "result", newDecision ? "processed" : "duplicate")).isEqualTo(1);
            assertThat(count(meters, "warexis.messaging.deliveries", "result", newDecision ? "duplicate" : "processed")).isZero();
            var order = inOrder(outcomes, deliveries, ack);
            order.verify(outcomes).accept(any());
            order.verify(deliveries).complete(source);
            order.verify(ack).acknowledge();
        }
    }

    @Test void completionFailureDoesNotCountBusinessSuccess() {
        var meters = new SimpleMeterRegistry();
        var outcomes = mock(FulfillmentService.class);
        when(outcomes.accept(any())).thenReturn(true);
        var deliveries = mock(DeliveryState.class);
        var source = validRecord();
        when(deliveries.load(source)).thenReturn(state("PROCESS", 0, 0));
        when(deliveries.begin(eq(source), any())).thenReturn(true);
        doThrow(new IllegalStateException("metadata unavailable")).when(deliveries).complete(source);
        var ack = mock(Acknowledgment.class);
        listener(outcomes, deliveries, mock(KafkaTemplate.class), meters).receive(source, ack, mock(Consumer.class));
        assertThat(count(meters, "warexis.messaging.deliveries", "result", "processed")).isZero();
        verifyNoInteractions(ack);
    }

    @Test void processingRetryCountsOnlyTheActualRepeatedAttempt() {
        var meters = new SimpleMeterRegistry();
        var outcomes = mock(FulfillmentService.class);
        when(outcomes.accept(any())).thenThrow(new IllegalStateException()).thenReturn(true);
        var deliveries = mock(DeliveryState.class);
        var source = validRecord();
        when(deliveries.load(source)).thenReturn(state("PROCESS", 0, 0), state("PROCESS", 1, 0), state("COMPLETE", 2, 0));
        when(deliveries.begin(eq(source), any())).thenReturn(true);
        listener(outcomes, deliveries, mock(KafkaTemplate.class), meters).receive(source, mock(Acknowledgment.class), mock(Consumer.class));
        assertThat(count(meters, "warexis.messaging.failures", "stage", "process")).isEqualTo(1);
        assertThat(count(meters, "warexis.messaging.retries", "stage", "process")).isEqualTo(1);
        assertThat(count(meters, "warexis.messaging.deliveries", "result", "processed")).isEqualTo(1);
    }

    @Test void completedSourceReplayCountsOnlyAfterAcknowledgmentSucceeds() {
        var meters = new SimpleMeterRegistry();
        var deliveries = mock(DeliveryState.class);
        var source = validRecord();
        when(deliveries.load(source)).thenReturn(state("COMPLETE", 1, 0));
        var ack = mock(Acknowledgment.class);
        listener(mock(FulfillmentService.class), deliveries, mock(KafkaTemplate.class), meters).receive(source, ack, mock(Consumer.class));
        assertThat(count(meters, "warexis.messaging.deliveries", "result", "duplicate")).isEqualTo(1);
        doThrow(new IllegalStateException()).when(ack).acknowledge();
        listener(mock(FulfillmentService.class), deliveries, mock(KafkaTemplate.class), meters).receive(source, ack, mock(Consumer.class));
        assertThat(count(meters, "warexis.messaging.deliveries", "result", "duplicate")).isEqualTo(1);
    }

    @Test void poisonAndDltRetryAreActualAttemptsAndDltCompletionRequiresMetadata() {
        var meters = new SimpleMeterRegistry();
        var deliveries = mock(DeliveryState.class);
        var source = new ConsumerRecord<byte[], byte[]>(KafkaConfiguration.ORDER_TOPIC, 0, 4, new byte[0], new byte[]{'{'});
        when(deliveries.load(source)).thenReturn(state("PROCESS", 0, 0), state("DLT", 1, 0), state("DLT", 1, 1), state("COMPLETE", 1, 2));
        when(deliveries.begin(eq(source), any())).thenReturn(true);
        var kafka = mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.failedFuture(new IllegalStateException()), CompletableFuture.completedFuture(null));
        listener(mock(FulfillmentService.class), deliveries, kafka, meters).receive(source, mock(Acknowledgment.class), mock(Consumer.class));
        assertThat(count(meters, "warexis.messaging.failures", "stage", "process")).isEqualTo(1);
        assertThat(count(meters, "warexis.messaging.failures", "stage", "dlt")).isEqualTo(1);
        assertThat(count(meters, "warexis.messaging.retries", "stage", "process")).isZero();
        assertThat(count(meters, "warexis.messaging.retries", "stage", "dlt")).isEqualTo(1);
        assertThat(count(meters, "warexis.messaging.deliveries", "result", "dlt")).isEqualTo(1);
        assertThat(count(meters, "warexis.messaging.deliveries", "result", "duplicate")).isZero();
    }

    @Test void unavailableOrDisabledRegistryCannotAlterProcessing() {
        var broken = mock(MeterRegistry.class);
        when(broken.counter(anyString(), any(String[].class))).thenThrow(new IllegalStateException("registry unavailable"));
        for (MeterRegistry registry : new MeterRegistry[]{null, broken}) {
            var outcomes = mock(FulfillmentService.class);
            when(outcomes.accept(any())).thenReturn(true);
            var deliveries = mock(DeliveryState.class);
            var source = validRecord();
            when(deliveries.load(source)).thenReturn(state("PROCESS", 0, 0), state("COMPLETE", 1, 0));
            when(deliveries.begin(eq(source), any())).thenReturn(true);
            var ack = mock(Acknowledgment.class);
            listener(outcomes, deliveries, mock(KafkaTemplate.class), registry).receive(source, ack, mock(Consumer.class));
            verify(ack).acknowledge();
            verify(deliveries).complete(source);
        }
    }

    @Test void knownValidUnsubscribedEventIsIgnoredWithoutBusinessAttemptOrRetry() {
        var meters = new SimpleMeterRegistry();
        var deliveries = mock(DeliveryState.class);
        var business = mock(FulfillmentService.class);
        var source = validRecord();
        var root = (tools.jackson.databind.node.ObjectNode) tools.jackson.databind.json.JsonMapper.builder().build().readTree(source.value());
        root.put("eventType", "OrderPlaced");
        root.remove("causationId");
        ((tools.jackson.databind.node.ObjectNode) root.path("payload")).remove("reservationId");
        var ignored = new ConsumerRecord<byte[], byte[]>(source.topic(), source.partition(), source.offset(), source.key(), root.toString().getBytes(StandardCharsets.UTF_8));
        when(deliveries.load(ignored)).thenReturn(state("PROCESS", 0, 0), state("COMPLETE", 0, 0));
        var ack = mock(Acknowledgment.class);
        listener(business, deliveries, mock(KafkaTemplate.class), meters).receive(ignored, ack, mock(Consumer.class));
        assertThat(count(meters, "warexis.messaging.deliveries", "result", "ignored")).isEqualTo(1);
        assertThat(count(meters, "warexis.messaging.deliveries", "result", "duplicate")).isZero();
        assertThat(count(meters, "warexis.messaging.retries", "stage", "process")).isZero();
        verify(deliveries, never()).begin(any(), any());
        verifyNoInteractions(business);
        verify(ack).acknowledge();
        when(deliveries.load(ignored)).thenReturn(state("PROCESS", 0, 0));
        doThrow(new IllegalStateException()).when(deliveries).complete(ignored);
        listener(business, deliveries, mock(KafkaTemplate.class), meters).receive(ignored, mock(Acknowledgment.class), mock(Consumer.class));
        assertThat(count(meters, "warexis.messaging.deliveries", "result", "ignored")).isEqualTo(1);
    }

    @Test void dltMetadataFailureDoesNotCountCompletionAndWaitStatesDoNotCountAttempts() {
        var meters = new SimpleMeterRegistry();
        var deliveries = mock(DeliveryState.class);
        var source = validRecord();
        when(deliveries.load(source)).thenReturn(state("DLT", 1, 0));
        when(deliveries.begin(eq(source), any())).thenReturn(true);
        doThrow(new IllegalStateException()).when(deliveries).complete(source);
        var kafka = mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        var ack = mock(Acknowledgment.class);
        listener(mock(FulfillmentService.class), deliveries, kafka, meters).receive(source, ack, mock(Consumer.class));
        assertThat(count(meters, "warexis.messaging.deliveries", "result", "dlt")).isZero();
        assertThat(count(meters, "warexis.messaging.retries", "stage", "dlt")).isZero();
        verifyNoInteractions(ack);
        when(deliveries.load(source)).thenReturn(state("PAUSED", 4, 4));
        listener(mock(FulfillmentService.class), deliveries, kafka, meters).receive(source, ack, mock(Consumer.class));
        verify(kafka, times(1)).send(any(ProducerRecord.class));
        assertThat(count(meters, "warexis.messaging.retries", "stage", "dlt")).isZero();
    }

    @Test void terminalCountersObserveCommitAndNeverRolledBackOutcomes() {
        var registry = new SimpleMeterRegistry();
        var metrics = new OperationalMetrics(registry, true);
        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
        org.springframework.transaction.support.TransactionSynchronizationManager.initSynchronization();
        try {
            metrics.processed(true); metrics.published(); metrics.dlt(); metrics.ignored();
            assertThat(count(registry, "warexis.messaging.deliveries", "result", "processed")).isZero();
            for (var sync : org.springframework.transaction.support.TransactionSynchronizationManager.getSynchronizations())
                sync.afterCompletion(org.springframework.transaction.support.TransactionSynchronization.STATUS_ROLLED_BACK);
            assertThat(count(registry, "warexis.outbox.publications", "result", "published")).isZero();
            assertThat(count(registry, "warexis.messaging.deliveries", "result", "dlt")).isZero();
            assertThat(count(registry, "warexis.messaging.deliveries", "result", "ignored")).isZero();
        } finally {
            org.springframework.transaction.support.TransactionSynchronizationManager.clearSynchronization();
            org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(false);
        }
        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
        org.springframework.transaction.support.TransactionSynchronizationManager.initSynchronization();
        try {
            metrics.processed(true);
            for (var sync : org.springframework.transaction.support.TransactionSynchronizationManager.getSynchronizations()) sync.afterCommit();
            assertThat(count(registry, "warexis.messaging.deliveries", "result", "processed")).isEqualTo(1);
        } finally {
            org.springframework.transaction.support.TransactionSynchronizationManager.clearSynchronization();
            org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    @Test void allCustomLabelVariantsAreFixedAndHaveNoIdentifiers() {
        var registry = new SimpleMeterRegistry();
        var metrics = new OperationalMetrics(registry, true);
        metrics.published(); metrics.publicationFailed(); metrics.publicationRetried(); metrics.processed(true);
        metrics.processed(false); metrics.dlt(); metrics.processFailed(); metrics.dltFailed(); metrics.processRetried(); metrics.dltRetried(); metrics.paused();
        assertThat(registry.getMeters()).hasSize(12);
        assertThat(registry.getMeters()).allSatisfy(meter -> assertThat(meter.getId().getTags()).allSatisfy(tag -> {
            assertThat(tag.getKey()).isIn("result", "stage");
            assertThat(tag.getValue()).isIn("published", "failed", "processed", "duplicate", "ignored", "dlt", "process");
        }));
    }

    @SuppressWarnings("unchecked")
    private ConfirmationListener listener(FulfillmentService outcomes, DeliveryState deliveries,
            KafkaTemplate kafka, MeterRegistry meters) {
        return new ConfirmationListener(outcomes, deliveries, kafka, mock(KafkaListenerEndpointRegistry.class), new OperationalMetrics(meters, true));
    }

    private double count(MeterRegistry registry, String name, String key, String value) {
        return registry.get(name).tag(key, value).counter().count();
    }

    private DeliveryState.State state(String phase, int process, int dlt) { return new DeliveryState.State(phase, process, dlt, null, "INVALID_OR_CONFLICTING"); }

    private ConsumerRecord<byte[], byte[]> validRecord() {
        var order = UUID.randomUUID(); var reservation = UUID.randomUUID();
        var json = """
                {"eventId":"%s","eventType":"OrderConfirmed","eventVersion":1,
                "occurredAt":"2026-10-07T00:00:00Z","aggregateId":"%s","correlationId":"%s",
                "causationId":"%s","payload":{"orderId":"%s","reservationId":"%s",
                "lines":[{"orderLineId":"%s","sku":"A","quantity":1}]}}
                """.formatted(UUID.randomUUID(), order, order, UUID.randomUUID(), order, reservation, UUID.randomUUID());
        return new ConsumerRecord<>(KafkaConfiguration.ORDER_TOPIC, 0, 3, order.toString().getBytes(StandardCharsets.UTF_8), json.getBytes(StandardCharsets.UTF_8));
    }
}
