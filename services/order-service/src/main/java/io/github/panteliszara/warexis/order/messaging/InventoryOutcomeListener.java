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
        InventoryOutcome outcome = null;
        int attempt = 0;
        try {
            while (true) {
                var state = deliveries.load(record);
                attempt = state.phase().equals("PROCESS")
                        || state.phase().equals("COMPLETE") && state.dltAttempts() == 0
                        ? state.processingAttempts() : state.dltAttempts();
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
                        logOutcome("PAUSED", state.dltAttempts(), outcome, record);
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
                attempt++;
                if (state.phase().equals("PROCESS")) {
                    try {
                        outcome = EventJson.outcome(record.key(), record.value());
                        outcomes.apply(outcome);
                    } catch (PoisonMessageException ex) {
                        logOutcome("DLT", state.processingAttempts() + 1, outcome, record);
                        deliveries.toDlt(record, "INVALID_OR_CONFLICTING");
                        continue;
                    } catch (Exception ex) {
                        logOutcome("PROCESS_RETRY", state.processingAttempts() + 1, outcome, record);
                        continue;
                    }
                    // A completion-metadata failure must escape to fail-closed pause, not spend another business attempt.
                    deliveries.complete(record);
                    logOutcome("COMPLETE", state.processingAttempts() + 1, outcome, record);
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
                        logOutcome("DLT_RETRY", state.dltAttempts() + 1, outcome, record);
                        continue;
                    }
                    // The broker acknowledged; failure to checkpoint that fact must pause without another send.
                    deliveries.complete(record);
                    logOutcome("DLT_COMPLETE", state.dltAttempts() + 1, outcome, record);
                }
            }
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            logOutcome("PAUSED", attempt, outcome, record);
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

    private void logOutcome(String state, int attempt, InventoryOutcome outcome, ConsumerRecord<byte[], byte[]> record) {
        // Only fully validated typed identities are logged; malformed transport uses source coordinates.
        LOG.atLevel(state.equals("COMPLETE") || state.equals("DLT_COMPLETE")
                ? org.slf4j.event.Level.INFO : org.slf4j.event.Level.WARN).log("Order inventory delivery; eventId={}, eventType={}, orderId={}, state={}, attempt={}, partition={}, offset={}",
                outcome == null ? null : outcome.eventId(), outcome == null ? null : outcome.eventType(),
                outcome == null ? null : outcome.orderId(), state, attempt, record.partition(), record.offset());
    }

    private void header(ProducerRecord<byte[], byte[]> record, String key, String value) {
        record.headers().add(key, value.getBytes(StandardCharsets.UTF_8));
    }
}
