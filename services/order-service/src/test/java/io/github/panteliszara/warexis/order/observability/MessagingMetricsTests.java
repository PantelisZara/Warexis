package io.github.panteliszara.warexis.order.observability;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.github.panteliszara.warexis.order.messaging.*;
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
            var outcomes = mock(InventoryOutcomeService.class);
            when(outcomes.apply(any())).thenReturn(newDecision);
            var deliveries = mock(DeliveryState.class);
            var source = validRecord();
            when(deliveries.load(source)).thenReturn(state("PROCESS", 0, 0), state("COMPLETE", 1, 0));
            when(deliveries.begin(eq(source), any())).thenReturn(true);
            var ack = mock(Acknowledgment.class);
            listener(outcomes, deliveries, mock(KafkaTemplate.class), meters).receive(source, ack, mock(Consumer.class));
            assertThat(count(meters, "warexis.messaging.deliveries", "result", newDecision ? "processed" : "duplicate")).isEqualTo(1);
            assertThat(count(meters, "warexis.messaging.deliveries", "result", newDecision ? "duplicate" : "processed")).isZero();
            var order = inOrder(outcomes, deliveries, ack);
            order.verify(outcomes).apply(any());
            order.verify(deliveries).complete(source);
            order.verify(ack).acknowledge();
        }
    }

    @Test void completionFailureDoesNotCountBusinessSuccess() {
        var meters = new SimpleMeterRegistry();
        var outcomes = mock(InventoryOutcomeService.class);
        when(outcomes.apply(any())).thenReturn(true);
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
        var outcomes = mock(InventoryOutcomeService.class);
        when(outcomes.apply(any())).thenThrow(new IllegalStateException()).thenReturn(true);
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
        listener(mock(InventoryOutcomeService.class), deliveries, mock(KafkaTemplate.class), meters).receive(source, ack, mock(Consumer.class));
        assertThat(count(meters, "warexis.messaging.deliveries", "result", "duplicate")).isEqualTo(1);
        doThrow(new IllegalStateException()).when(ack).acknowledge();
        listener(mock(InventoryOutcomeService.class), deliveries, mock(KafkaTemplate.class), meters).receive(source, ack, mock(Consumer.class));
        assertThat(count(meters, "warexis.messaging.deliveries", "result", "duplicate")).isEqualTo(1);
    }

    @Test void poisonAndDltRetryAreActualAttemptsAndDltCompletionRequiresMetadata() {
        var meters = new SimpleMeterRegistry();
        var deliveries = mock(DeliveryState.class);
        var source = new ConsumerRecord<byte[], byte[]>(KafkaConfiguration.INVENTORY_TOPIC, 0, 4, new byte[0], new byte[]{'{'});
        when(deliveries.load(source)).thenReturn(state("PROCESS", 0, 0), state("DLT", 1, 0), state("DLT", 1, 1), state("COMPLETE", 1, 2));
        when(deliveries.begin(eq(source), any())).thenReturn(true);
        var kafka = mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.failedFuture(new IllegalStateException()), CompletableFuture.completedFuture(null));
        listener(mock(InventoryOutcomeService.class), deliveries, kafka, meters).receive(source, mock(Acknowledgment.class), mock(Consumer.class));
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
            var outcomes = mock(InventoryOutcomeService.class);
            when(outcomes.apply(any())).thenReturn(true);
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
    private InventoryOutcomeListener listener(InventoryOutcomeService outcomes, DeliveryState deliveries,
            KafkaTemplate kafka, MeterRegistry meters) {
        return new InventoryOutcomeListener(outcomes, deliveries, kafka, mock(KafkaListenerEndpointRegistry.class), new OperationalMetrics(meters, true));
    }

    private double count(MeterRegistry registry, String name, String key, String value) {
        return registry.get(name).tag(key, value).counter().count();
    }

    private DeliveryState.State state(String phase, int process, int dlt) { return new DeliveryState.State(phase, process, dlt, null, "INVALID_OR_CONFLICTING"); }

    private ConsumerRecord<byte[], byte[]> validRecord() {
        var order = UUID.randomUUID(); var reservation = UUID.randomUUID();
        var json = """
                {"eventId":"%s","eventType":"InventoryReserved","eventVersion":1,
                "occurredAt":"2026-10-07T00:00:00Z","aggregateId":"%s","correlationId":"%s",
                "causationId":"%s","payload":{"orderId":"%s","reservationId":"%s",
                "lines":[{"orderLineId":"%s","sku":"A","quantity":1}]}}
                """.formatted(UUID.randomUUID(), reservation, order, UUID.randomUUID(), order, reservation, UUID.randomUUID());
        return new ConsumerRecord<>(KafkaConfiguration.INVENTORY_TOPIC, 0, 3, order.toString().getBytes(StandardCharsets.UTF_8), json.getBytes(StandardCharsets.UTF_8));
    }
}
