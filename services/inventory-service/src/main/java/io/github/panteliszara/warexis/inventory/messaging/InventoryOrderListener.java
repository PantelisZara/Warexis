package io.github.panteliszara.warexis.inventory.messaging;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import io.github.panteliszara.warexis.inventory.reservations.application.ConflictingOrderPlacedException;
import io.github.panteliszara.warexis.inventory.reservations.application.InventoryReservationService;

@Component
@ConditionalOnProperty(name = "warexis.messaging.enabled", havingValue = "true")
public class InventoryOrderListener {
    public static final String DLT = "warexis.order.events.v1.inventory.dlt";
    private static final Logger log = LoggerFactory.getLogger(InventoryOrderListener.class);
    private final ConsumerDeliveryStore delivery;
    private final WorkflowEventValidator validator;
    private final InventoryReservationService reservations;
    private final KafkaTemplate<byte[], byte[]> kafka;

    public InventoryOrderListener(ConsumerDeliveryStore delivery, WorkflowEventValidator validator,
                                  InventoryReservationService reservations, KafkaTemplate<byte[], byte[]> kafka) {
        this.delivery = delivery;
        this.validator = validator;
        this.reservations = reservations;
        this.kafka = kafka;
    }

    @KafkaListener(id = "inventory-orders", topics = "warexis.order.events.v1", groupId = ConsumerDeliveryStore.GROUP,
            containerFactory = "inventoryKafkaListenerContainerFactory")
    public void receive(ConsumerRecord<byte[], byte[]> record, Acknowledgment acknowledgment, Consumer<?, ?> consumer) {
        try {
            while (true) {
                var state = delivery.load(record);
                if (state.phase().equals("COMPLETE")) { acknowledgment.acknowledge(); return; }
                if (state.phase().equals("PAUSED")) { pause(record, consumer); return; }
                if (state.phase().equals("DLT")) {
                    if (state.dltAttempts() >= 4) {
                        delivery.transition(record, "PAUSED", "DLT_UNAVAILABLE");
                        continue;
                    }
                    await(state.nextAttemptAt());
                    if (!delivery.beginAttempt(record, state, true)) continue;
                    try {
                        var quarantined = new ProducerRecord<byte[], byte[]>(DLT, record.partition(), record.key(), record.value());
                        header(quarantined, "warexis-source-topic", record.topic());
                        header(quarantined, "warexis-source-partition", Integer.toString(record.partition()));
                        header(quarantined, "warexis-source-offset", Long.toString(record.offset()));
                        header(quarantined, "warexis-consumer-group", ConsumerDeliveryStore.GROUP);
                        header(quarantined, "warexis-failure-code", state.failureCode());
                        header(quarantined, "warexis-processing-attempts", Integer.toString(state.processingAttempts()));
                        header(quarantined, "warexis-dlt-attempts", Integer.toString(state.dltAttempts() + 1));
                        header(quarantined, "warexis-failed-at", Instant.now().toString());
                        kafka.send(quarantined).get(11, TimeUnit.SECONDS);
                    } catch (Exception exception) {
                        if (exception instanceof InterruptedException) { Thread.currentThread().interrupt(); pause(record, consumer); return; }
                        continue;
                    }
                    delivery.transition(record, "COMPLETE", state.failureCode());
                    continue;
                }
                WorkflowEventValidator.Validated event;
                try { event = validator.validate(record.key(), record.value()); }
                catch (InvalidEventException exception) { delivery.transition(record, "DLT", "INVALID_CONTRACT"); continue; }
                if (!event.eventType().equals("OrderPlaced")) {
                    delivery.transition(record, "COMPLETE", null);
                    continue;
                }
                if (state.processingAttempts() >= 4) { delivery.transition(record, "DLT", "RETRY_EXHAUSTED"); continue; }
                await(state.nextAttemptAt());
                if (!delivery.beginAttempt(record, state, false)) continue;
                try {
                    reservations.process(event.order());
                } catch (ConflictingOrderPlacedException exception) {
                    delivery.transition(record, "DLT", "IDENTITY_CONFLICT");
                    continue;
                } catch (RuntimeException exception) {
                    continue;
                }
                delivery.transition(record, "COMPLETE", null);
                // Reload the durable terminal state before acknowledging, including a lost ownership race.
                continue;
            }
        } catch (RuntimeException exception) {
            pause(record, consumer);
            log.error("Inventory consumer paused: delivery metadata or offset confirmation unavailable.");
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            pause(record, consumer);
        }
    }

    private void pause(ConsumerRecord<byte[], byte[]> record, Consumer<?, ?> consumer) {
        var partition = new TopicPartition(record.topic(), record.partition());
        consumer.seek(partition, record.offset());
        consumer.pause(Set.of(partition));
    }

    private void await(Instant due) throws InterruptedException {
        if (due != null) {
            var remaining = java.time.Duration.between(Instant.now(), due).toMillis();
            if (remaining > 0) Thread.sleep(remaining);
        }
    }

    private void header(ProducerRecord<byte[], byte[]> record, String key, String value) {
        record.headers().add(key, value.getBytes(StandardCharsets.UTF_8));
    }
}
