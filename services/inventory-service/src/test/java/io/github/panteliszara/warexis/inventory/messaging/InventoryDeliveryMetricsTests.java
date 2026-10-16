package io.github.panteliszara.warexis.inventory.messaging;

import io.github.panteliszara.warexis.inventory.observability.InventoryMetrics;
import io.github.panteliszara.warexis.inventory.reservations.application.InventoryReservationService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.CompletableFuture;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class InventoryDeliveryMetricsTests {
    private final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("warexis.order.events.v1", 0, 1L, new byte[]{1}, new byte[]{2});
    private final ConsumerDeliveryStore delivery = mock(ConsumerDeliveryStore.class);
    private final WorkflowEventValidator validator = mock(WorkflowEventValidator.class);
    private final InventoryReservationService reservations = mock(InventoryReservationService.class);
    @SuppressWarnings("unchecked") private final KafkaTemplate<byte[], byte[]> kafka = mock(KafkaTemplate.class);
    private final Acknowledgment acknowledgment = mock(Acknowledgment.class);
    private final Consumer<?, ?> consumer = mock(Consumer.class);
    private final ConsumerDeliveryStore.State process = new ConsumerDeliveryStore.State("PROCESS", 0, 0, null, null);
    private final ConsumerDeliveryStore.State complete = new ConsumerDeliveryStore.State("COMPLETE", 1, 0, null, null);

    @Test
    void alreadyCompleteCountsDuplicateOnlyAfterSuccessfulAck() {
        when(delivery.load(record)).thenReturn(complete);
        var registry = new SimpleMeterRegistry();
        try {
            var listener = listener(registry);
            listener.receive(record, acknowledgment, consumer);
            assertThat(counter(registry, "duplicate")).isEqualTo(1);
            doThrow(new IllegalStateException()).when(acknowledgment).acknowledge();
            listener.receive(record, acknowledgment, consumer);
            assertThat(counter(registry, "duplicate")).isEqualTo(1);
            assertThat(registry.get("warexis.messaging.pauses").counter().count()).isEqualTo(1);
        } finally { registry.close(); }
    }

    @Test
    void poisonDltRequiresBrokerAckDurableCompletionAndSourceAcknowledgment() {
        var dlt = new ConsumerDeliveryStore.State("DLT", 0, 1, null, "INVALID_CONTRACT");
        when(delivery.load(record)).thenReturn(process, dlt, complete);
        when(validator.validate(any(), any())).thenThrow(new InvalidEventException());
        when(delivery.transition(record, "DLT", "INVALID_CONTRACT")).thenReturn(true);
        when(delivery.transition(record, "COMPLETE", "INVALID_CONTRACT")).thenReturn(true);
        when(delivery.beginAttempt(record, dlt, true)).thenReturn(true);
        when(kafka.send(any(org.apache.kafka.clients.producer.ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        var registry = new SimpleMeterRegistry();
        try {
            listener(registry).receive(record, acknowledgment, consumer);
            assertThat(counter(registry, "dlt")).isEqualTo(1);
            assertThat(counter(registry, "processed")).isZero();
            assertThat(registry.get("warexis.messaging.failures").tag("stage", "process").counter().count()).isEqualTo(1);
            assertThat(registry.get("warexis.messaging.retries").tag("stage", "dlt").counter().count()).isEqualTo(1);
        } finally { registry.close(); }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("unchecked")
    void assignmentPausesPreserveRecoveryWithoutCountingListenerPauseRequests(boolean metadataUnavailable) {
        var partition = new TopicPartition(record.topic(), record.partition());
        if (metadataUnavailable) {
            when(delivery.pausedOffset(partition)).thenThrow(new IllegalStateException("metadata unavailable"));
        } else {
            when(delivery.pausedOffset(partition)).thenReturn(OptionalLong.of(record.offset()));
        }
        var registry = new SimpleMeterRegistry();
        try {
            var listener = listener(registry);
            var factory = new InventoryKafkaConfiguration().inventoryKafkaListenerContainerFactory(
                    mock(ConsumerFactory.class), delivery, false);
            assertThat(factory.getContainerProperties().isObservationEnabled()).isFalse();
            assertThat(factory.getContainerProperties().isMicrometerEnabled()).isTrue();
            var rebalance = (ConsumerAwareRebalanceListener) factory.getContainerProperties().getConsumerRebalanceListener();
            rebalance.onPartitionsAssigned(consumer, List.of(partition));
            verify(consumer).pause(metadataUnavailable ? List.of(partition) : Set.of(partition));
            if (metadataUnavailable) verify(consumer, never()).seek(any(TopicPartition.class), anyLong());
            else verify(consumer).seek(partition, record.offset());
            assertThat(registry.get("warexis.messaging.pauses").counter().count()).isZero();

            when(delivery.load(record)).thenReturn(new ConsumerDeliveryStore.State("PAUSED", 4, 4, null, "DLT_UNAVAILABLE"));
            listener.receive(record, acknowledgment, consumer);
            assertThat(registry.get("warexis.messaging.pauses").counter().count()).isEqualTo(1);
            verify(acknowledgment, never()).acknowledge();
        } finally { registry.close(); }
    }

    private InventoryOrderListener listener(SimpleMeterRegistry registry) {
        return new InventoryOrderListener(delivery, validator, reservations, kafka, new InventoryMetrics(registry));
    }
    private double counter(SimpleMeterRegistry registry, String result) {
        return registry.get("warexis.messaging.deliveries").tag("result", result).counter().count();
    }
}
