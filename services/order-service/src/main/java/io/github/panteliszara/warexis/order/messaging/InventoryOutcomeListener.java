package io.github.panteliszara.warexis.order.messaging;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "warexis.messaging.enabled", havingValue = "true")
public class InventoryOutcomeListener {
    private static final Logger LOG = LoggerFactory.getLogger(InventoryOutcomeListener.class);
    private final InventoryOutcomeService outcomes;
    private final DeliveryState deliveries;
    private final KafkaTemplate<byte[], byte[]> kafka;
    private final KafkaListenerEndpointRegistry registry;

    public InventoryOutcomeListener(InventoryOutcomeService outcomes, DeliveryState deliveries,
            KafkaTemplate<byte[], byte[]> kafka, KafkaListenerEndpointRegistry registry) {
        this.outcomes = outcomes;
        this.deliveries = deliveries;
        this.kafka = kafka;
        this.registry = registry;
    }

    @KafkaListener(id="order-inventory", topics=KafkaConfiguration.INVENTORY_TOPIC, groupId=KafkaConfiguration.GROUP)
    public void receive(ConsumerRecord<byte[], byte[]> record, Acknowledgment ack, Consumer<byte[], byte[]> consumer) {
        try {
            while (true) {
                var state = deliveries.load(record);
                if (state.phase().equals("COMPLETE")) {
                    ack.acknowledge();
                    return;
                }
                if (state.phase().equals("PAUSED")) {
                    pause(record, consumer);
                    return;
                }
                if (state.phase().equals("PROCESS") && state.processingAttempts() >= 4) {
                    deliveries.toDlt(record, "TECHNICAL_EXHAUSTED");
                    continue;
                }
                if (state.phase().equals("DLT") && state.dltAttempts() >= 4) {
                    if (deliveries.pause(record)) {
                        pause(record, consumer);
                        return;
                    }
                    continue;
                }
                if (state.nextAt() != null) {
                    long wait = state.nextAt().toInstant().toEpochMilli() - Instant.now().toEpochMilli();
                    if (wait > 0) {
                        Thread.sleep(wait);
                    }
                }
                if (!deliveries.begin(record, state)) {
                    continue;
                }
                if (state.phase().equals("PROCESS")) {
                    try {
                        var outcome = EventJson.outcome(record.key(), record.value());
                        outcomes.apply(outcome);
                    } catch (PoisonMessageException ex) {
                        deliveries.toDlt(record, "INVALID_OR_CONFLICTING");
                        continue;
                    } catch (Exception ex) {
                        LOG.warn("Order inventory processing failed; partition={}, offset={}", record.partition(), record.offset());
                        continue;
                    }
                    // A completion-metadata failure must escape to fail-closed pause, not spend another business attempt.
                    deliveries.complete(record);
                } else {
                    var dlt = new ProducerRecord<byte[], byte[]>(KafkaConfiguration.DLT, record.partition(), record.key(), record.value());
                    header(dlt, "warexis-source-topic", record.topic());
                    header(dlt, "warexis-source-partition", Integer.toString(record.partition()));
                    header(dlt, "warexis-source-offset", Long.toString(record.offset()));
                    header(dlt, "warexis-consumer-group", KafkaConfiguration.GROUP);
                    header(dlt, "warexis-failure-code", state.failure());
                    header(dlt, "warexis-processing-attempts", Integer.toString(state.processingAttempts()));
                    header(dlt, "warexis-dlt-attempts", Integer.toString(state.dltAttempts() + 1));
                    header(dlt, "warexis-failed-at", Instant.now().toString());
                    try {
                        kafka.send(dlt).get(12, TimeUnit.SECONDS);
                    } catch (InterruptedException ex) {
                        throw ex;
                    } catch (Exception ex) {
                        LOG.warn("Order DLT publication failed; partition={}, offset={}", record.partition(), record.offset());
                        continue;
                    }
                    // The broker acknowledged; failure to checkpoint that fact must pause without another send.
                    deliveries.complete(record);
                }
            }
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            // Fail closed if retry persistence or source acknowledgment is unavailable.
            pause(record, consumer);
        }
    }

    private void pause(ConsumerRecord<byte[], byte[]> record, Consumer<byte[], byte[]> consumer) {
        var partition = new TopicPartition(record.topic(), record.partition());
        consumer.seek(partition, record.offset());
        var container = registry.getListenerContainer("order-inventory");
        if (container != null) {
            container.pausePartition(partition);
        }
        LOG.error("Order inventory partition paused; partition={}, offset={}", record.partition(), record.offset());
    }

    private void header(ProducerRecord<byte[], byte[]> record, String key, String value) {
        record.headers().add(key, value.getBytes(StandardCharsets.UTF_8));
    }
}
