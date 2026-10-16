package io.github.panteliszara.warexis.inventory.messaging;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.support.Acknowledgment;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;

import io.github.panteliszara.warexis.inventory.reservations.application.InventoryReservationService;
import io.github.panteliszara.warexis.inventory.reservations.application.OrderPlaced;
import io.github.panteliszara.warexis.inventory.reservations.domain.ReservationLine;
import io.github.panteliszara.warexis.inventory.stock.application.StockItemService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SpringBootTest(properties = {"warexis.messaging.enabled=true", "warexis.messaging.retry-base-delay=10ms",
        "warexis.messaging.poll-interval=24h", "spring.kafka.listener.auto-startup=false"})
@org.springframework.test.context.TestPropertySource(properties = {"WAREXIS_OAUTH2_ISSUER_URI=https://issuer.test/realms/warexis", "WAREXIS_OAUTH2_AUDIENCE=warexis-api"})
@org.springframework.context.annotation.Import(io.github.panteliszara.warexis.inventory.security.TestJwtConfiguration.class)
@Testcontainers
@org.junit.jupiter.api.extension.ExtendWith(org.springframework.boot.test.system.OutputCaptureExtension.class)
class InventoryMessagingIntegrationTests {
    @Container @ServiceConnection static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18.6");
    @Container @ServiceConnection static final KafkaContainer broker = new KafkaContainer("apache/kafka:4.2.2");

    @Autowired private KafkaListenerEndpointRegistry registry;
    @Autowired private InventoryOrderListener listener;
    @Autowired private InventoryOutboxPublisher publisher;
    @Autowired private InventoryReservationService reservations;
    @Autowired private StockItemService stocks;
    @Autowired private ConsumerDeliveryStore delivery;
    @Autowired private WorkflowEventValidator validator;
    @Autowired private MessagingProperties properties;
    @Autowired private JdbcTemplate jdbc;
    @Autowired private DataSource dataSource;
    @Autowired private KafkaTemplate<byte[], byte[]> kafka;
    @Autowired private JsonMapper mapper;
    @Autowired private io.micrometer.core.instrument.MeterRegistry meters;
    @Autowired private io.github.panteliszara.warexis.inventory.observability.InventoryMetrics metrics;
    private final AtomicInteger offset = new AtomicInteger();
    private static boolean producerReady;

    @BeforeEach
    void resetDisposableDatabase() throws Exception {
        awaitProducerReadiness();
        jdbc.execute("TRUNCATE outbox_events, processed_order_events, inventory_reservations, stock_items, consumer_delivery_state");
        jdbc.update("UPDATE outbox_relay_control SET paused = FALSE, failure_code = NULL");
    }

    @Test
    void listenerCommitsReservationBeforeAckAndPublisherWaitsForRealBrokerAck() throws Exception {
        var processed = deliveryCount("processed");
        var duplicates = deliveryCount("duplicate");
        var published = publicationCount("published");
        var sku = stock(5);
        var record = event(sku, 2);
        var acknowledgment = mock(Acknowledgment.class);
        doAnswer(call -> {
            assertThat(count("SELECT count(*) FROM inventory_reservations")).isEqualTo(1);
            assertThat(delivery.load(record).phase()).isEqualTo("COMPLETE");
            assertThat(stocks.get(sku).getReserved()).isEqualTo(2);
            return null;
        }).when(acknowledgment).acknowledge();
        listener.receive(record, acknowledgment, consumer());
        verify(acknowledgment).acknowledge();
        listener.receive(new ConsumerRecord<>(record.topic(), record.partition(), offset.incrementAndGet(), record.key(), record.value()), mock(Acknowledgment.class), consumer());
        assertThat(count("SELECT count(*) FROM inventory_reservations")).isEqualTo(1);
        assertThat(deliveryCount("processed") - processed).isEqualTo(1);
        assertThat(deliveryCount("duplicate") - duplicates).isEqualTo(1);
        publisher.publishNext();
        assertThat(publicationCount("published") - published).isEqualTo(1);
        var sent = consume("warexis.inventory.events.v1", record.key());
        var json = mapper.readTree(sent.value());
        assertThat(json.path("eventType").asString()).isEqualTo("InventoryReserved");
        assertThat(json.path("causationId").asString()).isEqualTo(mapper.readTree(record.value()).path("eventId").asString());
        assertThat(count("SELECT count(*) FROM outbox_events WHERE published_at IS NOT NULL AND publication_attempts = 1")).isEqualTo(1);
    }


    @Test
    void validatedConfirmationIsDurablyAcknowledgedWithoutAnyInventoryBusinessEffect() throws Exception {
        var sku = stock(5);
        var incoming = event(sku, 2);
        var json = (tools.jackson.databind.node.ObjectNode) mapper.readTree(incoming.value());
        json.put("eventType", "OrderConfirmed");
        json.put("causationId", UUID.randomUUID().toString());
        ((tools.jackson.databind.node.ObjectNode) json.path("payload")).put("reservationId", UUID.randomUUID().toString());
        var sent = kafka.send(new org.apache.kafka.clients.producer.ProducerRecord<byte[], byte[]>(
                "warexis.order.events.v1", 0, incoming.key(), mapper.writeValueAsBytes(json)))
                .get(15, TimeUnit.SECONDS).getRecordMetadata();
        var source = new ConsumerRecord<byte[], byte[]>(sent.topic(), sent.partition(), sent.offset(), incoming.key(), mapper.writeValueAsBytes(json));
        var container = registry.getListenerContainer("inventory-orders");
        // This class also publishes unrelated records in relay tests. Seek this isolated partition to
        // the confirmation before enabling the listener, so earlier test inputs cannot mutate stock.
        try (var admin = org.apache.kafka.clients.admin.Admin.create(Map.of("bootstrap.servers", broker.getBootstrapServers()))) {
            var offsets = new java.util.HashMap<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata>();
            var ends = admin.listOffsets(Map.of(new TopicPartition(source.topic(), 0), org.apache.kafka.clients.admin.OffsetSpec.latest(),
                    new TopicPartition(source.topic(), 1), org.apache.kafka.clients.admin.OffsetSpec.latest(),
                    new TopicPartition(source.topic(), 2), org.apache.kafka.clients.admin.OffsetSpec.latest())).all().get(10, TimeUnit.SECONDS);
            ends.forEach((partition, end) -> offsets.put(partition, new org.apache.kafka.clients.consumer.OffsetAndMetadata(end.offset())));
            offsets.put(new TopicPartition(source.topic(), 0), new org.apache.kafka.clients.consumer.OffsetAndMetadata(source.offset()));
            admin.alterConsumerGroupOffsets(ConsumerDeliveryStore.GROUP, offsets).all().get(10, TimeUnit.SECONDS);
            try {
                container.start();
                var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                boolean acknowledged = false;
                while (System.nanoTime() < deadline && !acknowledged) {
                    var committed = admin.listConsumerGroupOffsets(ConsumerDeliveryStore.GROUP).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
                    var offset = committed.get(new TopicPartition(source.topic(), source.partition()));
                    acknowledged = offset != null && offset.offset() > source.offset();
                    if (!acknowledged) Thread.sleep(25);
                }
                assertThat(acknowledged).as("real Kafka confirmation source acknowledged").isTrue();
            } finally { container.stop(); }
        }
        assertThat(delivery.load(source).phase()).isEqualTo("COMPLETE");
        assertThat(delivery.load(source).processingAttempts()).isZero();
        assertThat(delivery.load(source).dltAttempts()).isZero();
        assertThat(stocks.get(sku).getReserved()).isZero();
        assertThat(count("SELECT count(*) FROM inventory_reservations")).isZero();
        assertThat(count("SELECT count(*) FROM processed_order_events")).isZero();
        assertThat(count("SELECT count(*) FROM outbox_events")).isZero();
    }

    @Test
    void malformedConfirmationReachesRealDltRatherThanCompatibleSkip() throws Exception {
        var incoming = event(stock(5), 1);
        var json = (tools.jackson.databind.node.ObjectNode) mapper.readTree(incoming.value());
        json.put("eventType", "OrderConfirmed"); // Required reservationId and causationId absent.
        var source = new ConsumerRecord<byte[], byte[]>(incoming.topic(), incoming.partition(), offset.incrementAndGet(), incoming.key(), mapper.writeValueAsBytes(json));
        var acknowledgment = mock(Acknowledgment.class);
        listener.receive(source, acknowledgment, consumer());
        assertThat(consume(InventoryOrderListener.DLT, source.key()).value()).containsExactly(source.value());
        verify(acknowledgment).acknowledge();
        assertThat(delivery.load(source).phase()).isEqualTo("COMPLETE");
        assertThat(delivery.load(source).processingAttempts()).isZero();
        assertThat(count("SELECT count(*) FROM inventory_reservations")).isZero();
        assertThat(count("SELECT count(*) FROM processed_order_events")).isZero();
        assertThat(count("SELECT count(*) FROM outbox_events")).isZero();
    }

    @Test
    void businessRejectionIsSuccessfulProcessingWithoutRetry() {
        var record = event(stock(0), 2);
        var acknowledgment = mock(Acknowledgment.class);
        listener.receive(record, acknowledgment, consumer());
        verify(acknowledgment).acknowledge();
        assertThat(delivery.load(record).processingAttempts()).isEqualTo(1);
        assertThat(delivery.load(record).dltAttempts()).isZero();
        assertThat(jdbc.queryForObject("SELECT status FROM inventory_reservations", String.class)).isEqualTo("REJECTED");
    }

    @Test
    void poisonBytesArePreservedInRealDltWithSourceMetadataBeforeAck() throws Exception {
        var bytes = new byte[]{(byte) 0xc3, 0x28};
        var record = new ConsumerRecord<byte[], byte[]>("warexis.order.events.v1", 2, offset.incrementAndGet(), "bad".getBytes(StandardCharsets.UTF_8), bytes);
        var acknowledgment = mock(Acknowledgment.class);
        listener.receive(record, acknowledgment, consumer());
        var sent = consume(InventoryOrderListener.DLT, record.key());
        assertThat(sent.value()).containsExactly(bytes);
        assertThat(sent.partition()).isEqualTo(2);
        assertThat(new String(sent.headers().lastHeader("warexis-source-offset").value(), StandardCharsets.UTF_8)).isEqualTo(Long.toString(record.offset()));
        assertThat(new String(sent.headers().lastHeader("warexis-consumer-group").value(), StandardCharsets.UTF_8)).isEqualTo(ConsumerDeliveryStore.GROUP);
        verify(acknowledgment).acknowledge();
        assertThat(delivery.load(record).processingAttempts()).isZero();
        assertThat(count("SELECT count(*) FROM inventory_reservations")).isZero();
    }

    @Test
    void transientBudgetSurvivesReconstructedListenerAndExhaustionReachesRealDlt() throws Exception {
        var record = event(stock(2), 1);
        delivery.load(record);
        jdbc.update("UPDATE consumer_delivery_state SET processing_attempts = 2 WHERE source_offset = ?", record.offset());
        var failingBusiness = mock(InventoryReservationService.class);
        when(failingBusiness.processWithOutcome(any())).thenThrow(new IllegalStateException("synthetic technical failure"));
        var restarted = new InventoryOrderListener(delivery, validator, failingBusiness, kafka);
        var acknowledgment = mock(Acknowledgment.class);
        restarted.receive(record, acknowledgment, consumer());
        verify(failingBusiness, times(2)).processWithOutcome(any());
        assertThat(delivery.load(record).processingAttempts()).isEqualTo(4);
        assertThat(delivery.load(record).phase()).isEqualTo("COMPLETE");
        assertThat(consume(InventoryOrderListener.DLT, record.key()).value()).containsExactly(record.value());
        verify(acknowledgment).acknowledge();
    }

    @Test
    void boundedDltFailureDurablyPausesAndRestartDoesNotRetryOrAck() {
        var failures = meters.get("warexis.messaging.failures").tag("stage", "dlt").counter().count();
        var retries = meters.get("warexis.messaging.retries").tag("stage", "dlt").counter().count();
        var pauses = meters.get("warexis.messaging.pauses").counter().count();
        var record = new ConsumerRecord<byte[], byte[]>("warexis.order.events.v1", 1, offset.incrementAndGet(), new byte[]{1}, new byte[]{2});
        var failingKafka = failingKafka();
        var acknowledgment = mock(Acknowledgment.class);
        var consumer = consumer();
        var failed = new InventoryOrderListener(delivery, validator, reservations, failingKafka, metrics);
        failed.receive(record, acknowledgment, consumer);
        assertThat(delivery.load(record).phase()).isEqualTo("PAUSED");
        assertThat(delivery.load(record).dltAttempts()).isEqualTo(4);
        verify(failingKafka, times(4)).send(any(org.apache.kafka.clients.producer.ProducerRecord.class));
        new InventoryOrderListener(delivery, validator, reservations, failingKafka, metrics).receive(record, acknowledgment, consumer);
        verify(failingKafka, times(4)).send(any(org.apache.kafka.clients.producer.ProducerRecord.class));
        verify(acknowledgment, never()).acknowledge();
        verify(consumer, times(2)).seek(new TopicPartition(record.topic(), record.partition()), record.offset());
        assertThat(meters.get("warexis.messaging.failures").tag("stage", "dlt").counter().count() - failures).isEqualTo(4);
        assertThat(meters.get("warexis.messaging.retries").tag("stage", "dlt").counter().count() - retries).isEqualTo(3);
        assertThat(meters.get("warexis.messaging.pauses").counter().count() - pauses).isEqualTo(2);
        assertThat(delivery.pausedOffset(new TopicPartition(record.topic(), record.partition()))).hasValue(record.offset());
    }

    @Test
    void metadataFailureFailsClosedWithoutBusinessOrAck() {
        var record = event(stock(2), 1);
        var business = mock(InventoryReservationService.class);
        var acknowledgment = mock(Acknowledgment.class);
        var consumer = consumer();
        jdbc.execute("ALTER TABLE consumer_delivery_state ADD CONSTRAINT test_fail_delivery_metadata CHECK (processing_attempts = 0) NOT VALID");
        try {
            new InventoryOrderListener(delivery, validator, business, kafka, metrics).receive(record, acknowledgment, consumer);
        } finally {
            jdbc.execute("ALTER TABLE consumer_delivery_state DROP CONSTRAINT test_fail_delivery_metadata");
        }
        verify(business, never()).processWithOutcome(any());
        verify(acknowledgment, never()).acknowledge();
        verify(consumer).seek(new TopicPartition(record.topic(), record.partition()), record.offset());
        assertThat(delivery.load(record).processingAttempts()).isZero();
    }

    @Test
    void relayAttemptBudgetAndGlobalPauseSurvivePublisherRestartWithoutSkippingHead() throws Exception {
        var failures = publicationCount("failed");
        var retries = meters.get("warexis.outbox.retries").counter().count();
        reservations.process(input(stock(2), 1));
        reservations.process(input(stock(2), 1));
        var failingKafka = failingKafka();
        var failed = new InventoryOutboxPublisher(dataSource, failingKafka, properties, validator, metrics);
        for (int attempt = 0; attempt < 5; attempt++) {
            jdbc.update("UPDATE outbox_events SET next_attempt_at = NULL");
            failed.publishNext();
        }
        assertThat(jdbc.queryForObject("SELECT paused FROM outbox_relay_control", Boolean.class)).isTrue();
        assertThat(count("SELECT count(*) FROM outbox_events WHERE publication_attempts = 5 AND published_at IS NULL")).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM outbox_events WHERE publication_attempts = 0 AND published_at IS NULL")).isEqualTo(1);
        new InventoryOutboxPublisher(dataSource, failingKafka, properties, validator, metrics).publishNext();
        verify(failingKafka, times(5)).send(any(org.apache.kafka.clients.producer.ProducerRecord.class));
        assertThat(publicationCount("failed") - failures).isEqualTo(5);
        assertThat(meters.get("warexis.outbox.retries").counter().count() - retries).isEqualTo(4);
    }

    @Test
    void brokerAckFollowedByDatabaseFailureRepublishesSameEventIdentity() throws Exception {
        var published = publicationCount("published");
        var failed = publicationCount("failed");
        var retries = meters.get("warexis.outbox.retries").counter().count();
        var input = input(stock(2), 1);
        reservations.process(input);
        var key = input.orderId().toString().getBytes(StandardCharsets.UTF_8);
        jdbc.execute("ALTER TABLE outbox_events ADD CONSTRAINT test_fail_publication_mark CHECK (published_at IS NULL) NOT VALID");
        try { publisher.publishNext(); }
        finally { jdbc.execute("ALTER TABLE outbox_events DROP CONSTRAINT test_fail_publication_mark"); }
        assertThat(count("SELECT count(*) FROM outbox_events WHERE published_at IS NULL AND publication_attempts = 1")).isEqualTo(1);
        assertThat(publicationCount("published")).isEqualTo(published);
        assertThat(publicationCount("failed") - failed).isEqualTo(1);
        var first = consume("warexis.inventory.events.v1", key);
        jdbc.update("UPDATE outbox_events SET next_attempt_at = NULL");
        publisher.publishNext();
        assertThat(publicationCount("published") - published).isEqualTo(1);
        assertThat(meters.get("warexis.outbox.retries").counter().count() - retries).isEqualTo(1);
        var copies = consumeCopies("warexis.inventory.events.v1", key, 2);
        assertThat(copies.get(0).value()).containsExactly(first.value());
        assertThat(copies.get(1).value()).containsExactly(first.value());
        assertThat(copies.get(1).offset()).isGreaterThan(copies.get(0).offset());
        assertThat(count("SELECT count(*) FROM outbox_events WHERE published_at IS NOT NULL AND publication_attempts = 2")).isEqualTo(1);
    }

    @Test
    void durablePartitionPauseSurvivesRealPollsAndListenerRestartWithoutSkippingRecord() throws Exception {
        var incoming = event(stock(5), 1);
        var sent = kafka.send(new org.apache.kafka.clients.producer.ProducerRecord<byte[], byte[]>(
                "warexis.order.events.v1", 2, incoming.key(), incoming.value())).get(15, TimeUnit.SECONDS).getRecordMetadata();
        var source = new ConsumerRecord<byte[], byte[]>(sent.topic(), sent.partition(), sent.offset(), incoming.key(), incoming.value());
        delivery.load(source);
        jdbc.execute("ALTER TABLE consumer_delivery_state ADD CONSTRAINT test_real_pause_metadata CHECK (processing_attempts = 0) NOT VALID");
        kafka.send(new org.apache.kafka.clients.producer.ProducerRecord<byte[], byte[]>(
                "warexis.order.events.v1", 2, incoming.key(), incoming.value())).get(15, TimeUnit.SECONDS);
        var container = registry.getListenerContainer("inventory-orders");
        try {
            for (int restart = 0; restart < 2; restart++) {
                container.start();
                var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (container.getAssignedPartitions().size() != 3 && System.nanoTime() < deadline) Thread.sleep(25);
                assertThat(container.getAssignedPartitions()).hasSize(3);
                Thread.sleep(1200);
                assertThat(count("SELECT count(*) FROM inventory_reservations")).isZero();
                assertThat(delivery.load(source).phase()).isEqualTo(restart == 0 ? "PROCESS" : "PAUSED");
                try (var admin = org.apache.kafka.clients.admin.Admin.create(Map.of("bootstrap.servers", broker.getBootstrapServers()))) {
                    var committed = admin.listConsumerGroupOffsets(ConsumerDeliveryStore.GROUP).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
                    var offset = committed.get(new TopicPartition(source.topic(), source.partition()));
                    assertThat(offset == null || offset.offset() <= source.offset()).isTrue();
                }
                container.stop();
                if (restart == 0) {
                    jdbc.execute("ALTER TABLE consumer_delivery_state DROP CONSTRAINT test_real_pause_metadata");
                    jdbc.update("UPDATE consumer_delivery_state SET phase = 'PAUSED', failure_code = 'DLT_UNAVAILABLE' WHERE source_topic = ? AND source_partition = ? AND source_offset = ?",
                            source.topic(), source.partition(), source.offset());
                }
            }
        } finally {
            container.stop();
            jdbc.execute("ALTER TABLE consumer_delivery_state DROP CONSTRAINT IF EXISTS test_real_pause_metadata");
        }
    }

    @Test
    void conflictingImmutableEventGoesDirectlyToDltWithoutRepeatedBusinessAttempts() throws Exception {
        var sku = stock(5);
        var original = event(sku, 1);
        listener.receive(original, mock(Acknowledgment.class), consumer());
        var changed = mapper.readTree(original.value());
        ((tools.jackson.databind.node.ObjectNode) changed.path("payload").path("lines").get(0)).put("quantity", 2);
        var conflicting = new ConsumerRecord<byte[], byte[]>(original.topic(), original.partition(), offset.incrementAndGet(), original.key(), mapper.writeValueAsBytes(changed));
        var acknowledgment = mock(Acknowledgment.class);
        listener.receive(conflicting, acknowledgment, consumer());
        assertThat(delivery.load(conflicting).failureCode()).isEqualTo("IDENTITY_CONFLICT");
        assertThat(delivery.load(conflicting).processingAttempts()).isEqualTo(1);
        assertThat(consume(InventoryOrderListener.DLT, original.key()).value()).containsExactly(conflicting.value());
        assertThat(stocks.get(sku).getReserved()).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM inventory_reservations")).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM outbox_events")).isEqualTo(1);
        verify(acknowledgment).acknowledge();
    }

    @Test
    @SuppressWarnings("unchecked")
    void sessionGuardPreventsTwoRelaysPublishingTheSameHeadConcurrently() throws Exception {
        reservations.process(input(stock(2), 1));
        KafkaTemplate<byte[], byte[]> blockingKafka = mock(KafkaTemplate.class);
        var sent = new java.util.concurrent.CountDownLatch(1);
        var confirmation = new CompletableFuture<org.springframework.kafka.support.SendResult<byte[], byte[]>>();
        when(blockingKafka.send(any(org.apache.kafka.clients.producer.ProducerRecord.class))).thenAnswer(call -> {
            sent.countDown();
            return confirmation;
        });
        var first = new InventoryOutboxPublisher(dataSource, blockingKafka, properties, validator);
        var second = new InventoryOutboxPublisher(dataSource, blockingKafka, properties, validator);
        try (var executor = java.util.concurrent.Executors.newSingleThreadExecutor()) {
            var publication = executor.submit(first::publishNext);
            try {
                assertThat(sent.await(5, TimeUnit.SECONDS)).isTrue();
                second.publishNext();
                verify(blockingKafka, times(1)).send(any(org.apache.kafka.clients.producer.ProducerRecord.class));
                assertThat(count("SELECT count(*) FROM outbox_events WHERE publication_attempts = 1 AND published_at IS NULL")).isEqualTo(1);
            } finally { confirmation.complete(null); }
            publication.get(5, TimeUnit.SECONDS);
        }
        assertThat(count("SELECT count(*) FROM outbox_events WHERE publication_attempts = 1 AND published_at IS NOT NULL")).isEqualTo(1);
    }

    @Test
    void relayMetadataFailureCannotSendWithoutFirstPersistingItsAttempt() {
        reservations.process(input(stock(2), 1));
        var transport = failingKafka();
        jdbc.execute("ALTER TABLE outbox_events ADD CONSTRAINT test_fail_relay_attempt CHECK (publication_attempts = 0) NOT VALID");
        try { new InventoryOutboxPublisher(dataSource, transport, properties, validator).publishNext(); }
        finally { jdbc.execute("ALTER TABLE outbox_events DROP CONSTRAINT test_fail_relay_attempt"); }
        verify(transport, never()).send(any(org.apache.kafka.clients.producer.ProducerRecord.class));
        assertThat(count("SELECT count(*) FROM outbox_events WHERE publication_attempts = 0 AND published_at IS NULL")).isEqualTo(1);
    }

    @Test
    void staleProcessingAndDltSnapshotsCannotDowngradeOrClaimCompletedDelivery() {
        var processing = event(stock(2), 1);
        var staleProcessing = delivery.load(processing);
        assertThat(delivery.transition(processing, "COMPLETE", null)).isTrue();
        assertThat(delivery.beginAttempt(processing, staleProcessing, false)).isFalse();
        assertThat(delivery.transition(processing, "DLT", "RETRY_EXHAUSTED")).isFalse();
        assertThat(delivery.transition(processing, "PAUSED", "DLT_UNAVAILABLE")).isFalse();
        assertThat(delivery.load(processing).phase()).isEqualTo("COMPLETE");
        assertThat(delivery.load(processing).processingAttempts()).isZero();

        var quarantined = event(stock(2), 1);
        delivery.load(quarantined);
        jdbc.update("UPDATE consumer_delivery_state SET phase = 'DLT', dlt_attempts = 4 WHERE source_offset = ?", quarantined.offset());
        var staleDlt = delivery.load(quarantined);
        assertThat(delivery.transition(quarantined, "COMPLETE", "INVALID_CONTRACT")).isTrue();
        assertThat(delivery.beginAttempt(quarantined, staleDlt, true)).isFalse();
        assertThat(delivery.transition(quarantined, "PAUSED", "DLT_UNAVAILABLE")).isFalse();
        assertThat(delivery.load(quarantined).phase()).isEqualTo("COMPLETE");
        assertThat(delivery.load(quarantined).dltAttempts()).isEqualTo(4);
        assertThat(delivery.pausedOffset(new TopicPartition(quarantined.topic(), quarantined.partition()))).isEmpty();
        var acknowledgment = mock(Acknowledgment.class);
        var consumer = consumer();
        var racingStore = org.mockito.Mockito.spy(new ConsumerDeliveryStore(jdbc, properties));
        var firstRead = new java.util.concurrent.atomic.AtomicBoolean(true);
        doAnswer(call -> firstRead.getAndSet(false) ? staleDlt : call.callRealMethod())
                .when(racingStore).load(quarantined);
        var transport = failingKafka();
        new InventoryOrderListener(racingStore, validator, reservations, transport)
                .receive(quarantined, acknowledgment, consumer);
        verify(acknowledgment).acknowledge();
        verify(consumer, never()).pause(any());
        org.mockito.Mockito.verifyNoInteractions(transport);
    }

    @Test
    void concurrentObservedClaimsHaveOneWinnerAndCannotExceedDurableBudget() throws Exception {
        var record = event(stock(2), 1);
        try (var executor = java.util.concurrent.Executors.newFixedThreadPool(2)) {
            for (int attempt = 0; attempt < 5; attempt++) {
                var snapshot = delivery.load(record);
                var start = new java.util.concurrent.CountDownLatch(1);
                var first = executor.submit(() -> { start.await(); return delivery.beginAttempt(record, snapshot, false); });
                var second = executor.submit(() -> { start.await(); return delivery.beginAttempt(record, snapshot, false); });
                start.countDown();
                var claims = List.of(first.get(5, TimeUnit.SECONDS), second.get(5, TimeUnit.SECONDS));
                assertThat(claims.stream().filter(Boolean::booleanValue).count()).isEqualTo(attempt < 4 ? 1 : 0);
                assertThat(delivery.load(record).processingAttempts()).isEqualTo(Math.min(attempt + 1, 4));
            }
            assertThat(delivery.transition(record, "DLT", "RETRY_EXHAUSTED")).isTrue();
            var snapshot = delivery.load(record);
            var start = new java.util.concurrent.CountDownLatch(1);
            var first = executor.submit(() -> { start.await(); return delivery.beginAttempt(record, snapshot, true); });
            var second = executor.submit(() -> { start.await(); return delivery.beginAttempt(record, snapshot, true); });
            start.countDown();
            assertThat(List.of(first.get(5, TimeUnit.SECONDS), second.get(5, TimeUnit.SECONDS)))
                    .containsExactlyInAnyOrder(true, false);
            assertThat(delivery.load(record).dltAttempts()).isEqualTo(1);
        }
    }

    @Test
    void suppressedAttemptWriteFailsClosedWithoutBusinessOrAck() {
        var record = event(stock(2), 1);
        var business = mock(InventoryReservationService.class);
        var acknowledgment = mock(Acknowledgment.class);
        var consumer = consumer();
        suppressDeliveryUpdate("NEW.processing_attempts > OLD.processing_attempts");
        try {
            new InventoryOrderListener(delivery, validator, business, kafka, metrics).receive(record, acknowledgment, consumer);
        } finally { removeSuppressedDeliveryUpdate(); }
        verify(business, never()).processWithOutcome(any());
        verify(acknowledgment, never()).acknowledge();
        verify(consumer).pause(java.util.Set.of(new TopicPartition(record.topic(), record.partition())));
        assertThat(delivery.load(record).processingAttempts()).isZero();
    }

    @Test
    void suppressedCompletionAfterBusinessCommitDoesNotRepeatBusinessOrAck(org.springframework.boot.test.system.CapturedOutput output) {
        var processed = deliveryCount("processed");
        var sku = stock(3);
        var record = event(sku, 1);
        var business = mock(InventoryReservationService.class);
        when(business.processWithOutcome(any())).thenAnswer(call -> reservations.processWithOutcome(call.getArgument(0)));
        var acknowledgment = mock(Acknowledgment.class);
        var consumer = consumer();
        suppressDeliveryUpdate("NEW.phase = 'COMPLETE'");
        try {
            new InventoryOrderListener(delivery, validator, business, kafka, metrics).receive(record, acknowledgment, consumer);
        } finally { removeSuppressedDeliveryUpdate(); }
        assertThat(deliveryCount("processed")).isEqualTo(processed);
        verify(business, times(1)).processWithOutcome(any());
        verify(acknowledgment, never()).acknowledge();
        verify(consumer).seek(new TopicPartition(record.topic(), record.partition()), record.offset());
        verify(consumer).pause(java.util.Set.of(new TopicPartition(record.topic(), record.partition())));
        assertThat(delivery.load(record).phase()).isEqualTo("PROCESS");
        assertThat(delivery.load(record).processingAttempts()).isEqualTo(1);
        assertThat(delivery.load(record).dltAttempts()).isZero();
        assertThat(output.getOut()).contains("processingAttempt=1 dltAttempt=0 state=PAUSED classification=METADATA_OR_ACK_UNAVAILABLE");
        assertThat(stocks.get(sku).getReserved()).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM inventory_reservations")).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM outbox_events")).isEqualTo(1);
    }

    @Test
    @SuppressWarnings("unchecked")
    void suppressedCompletionAfterDltBrokerAckDoesNotResendOrAck(org.springframework.boot.test.system.CapturedOutput output) {
        var completedDlt = deliveryCount("dlt");
        var record = new ConsumerRecord<byte[], byte[]>("warexis.order.events.v1", 1, offset.incrementAndGet(),
                UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8), new byte[]{(byte) 0xc3, 0x28});
        KafkaTemplate<byte[], byte[]> transport = mock(KafkaTemplate.class);
        when(transport.send(any(org.apache.kafka.clients.producer.ProducerRecord.class)))
                .thenAnswer(call -> kafka.send(call.getArgument(0, org.apache.kafka.clients.producer.ProducerRecord.class)));
        var acknowledgment = mock(Acknowledgment.class);
        var consumer = consumer();
        suppressDeliveryUpdate("NEW.phase = 'COMPLETE'");
        try {
            new InventoryOrderListener(delivery, validator, reservations, transport, metrics).receive(record, acknowledgment, consumer);
        } finally { removeSuppressedDeliveryUpdate(); }
        assertThat(deliveryCount("dlt")).isEqualTo(completedDlt);
        verify(transport, times(1)).send(any(org.apache.kafka.clients.producer.ProducerRecord.class));
        verify(acknowledgment, never()).acknowledge();
        verify(consumer).pause(java.util.Set.of(new TopicPartition(record.topic(), record.partition())));
        assertThat(delivery.load(record).phase()).isEqualTo("DLT");
        assertThat(delivery.load(record).dltAttempts()).isEqualTo(1);
        assertThat(output.getOut()).contains("processingAttempt=0 dltAttempt=1 state=PAUSED classification=METADATA_OR_ACK_UNAVAILABLE");
        assertThat(consume(InventoryOrderListener.DLT, record.key()).value()).containsExactly(record.value());
        assertThat(count("SELECT count(*) FROM inventory_reservations")).isZero();
    }

    @Test
    void suppressedEligibleDltAndPauseTransitionsFailClosedRatherThanLoop() {
        var record = event(stock(2), 1);
        delivery.load(record);
        suppressDeliveryUpdate("NEW.phase = 'DLT' OR NEW.phase = 'PAUSED'");
        try {
            assertThatThrownBy(() -> delivery.transition(record, "DLT", "INVALID_CONTRACT"))
                    .isInstanceOf(org.springframework.dao.DataAccessException.class)
                    .hasRootCauseInstanceOf(IllegalStateException.class).hasMessage("Delivery transition was not persisted.");
            assertThat(delivery.load(record).phase()).isEqualTo("PROCESS");
        } finally { removeSuppressedDeliveryUpdate(); }
        jdbc.update("UPDATE consumer_delivery_state SET phase = 'DLT', dlt_attempts = 4 WHERE source_offset = ?", record.offset());
        suppressDeliveryUpdate("NEW.phase = 'PAUSED'");
        try {
            assertThatThrownBy(() -> delivery.transition(record, "PAUSED", "DLT_UNAVAILABLE"))
                    .isInstanceOf(org.springframework.dao.DataAccessException.class)
                    .hasRootCauseInstanceOf(IllegalStateException.class).hasMessage("Delivery transition was not persisted.");
            assertThat(delivery.load(record).phase()).isEqualTo("DLT");
            assertThat(delivery.load(record).dltAttempts()).isEqualTo(4);
        } finally { removeSuppressedDeliveryUpdate(); }
    }

    /** Topic creation can precede KRaft leader readiness; do not spend business attempt budgets on fixture startup. */
    private void awaitProducerReadiness() throws Exception {
        if (producerReady) return;
        Exception lastFailure = null;
        for (int attempt = 0; attempt < 5; attempt++) {
            try {
                kafka.send("warexis.inventory.tests.readiness", new byte[0], new byte[0]).get(5, TimeUnit.SECONDS);
                producerReady = true;
                return;
            } catch (java.util.concurrent.ExecutionException exception) {
                if (!(exception.getCause() instanceof org.apache.kafka.common.errors.RetriableException)) throw exception;
                lastFailure = exception;
            } catch (java.util.concurrent.TimeoutException exception) {
                lastFailure = exception;
            }
        }
        throw new IllegalStateException("Disposable Kafka producer did not become ready.", lastFailure);
    }

    private double deliveryCount(String result) {
        return meters.get("warexis.messaging.deliveries").tag("result", result).counter().count();
    }

    private double publicationCount(String result) {
        return meters.get("warexis.outbox.publications").tag("result", result).counter().count();
    }

    private void suppressDeliveryUpdate(String predicate) {
        jdbc.execute("CREATE FUNCTION test_suppress_delivery_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF "
                + predicate + " THEN RETURN NULL; END IF; RETURN NEW; END $$");
        jdbc.execute("CREATE TRIGGER test_suppress_delivery_update BEFORE UPDATE ON consumer_delivery_state "
                + "FOR EACH ROW EXECUTE FUNCTION test_suppress_delivery_update()");
    }

    private void removeSuppressedDeliveryUpdate() {
        jdbc.execute("DROP TRIGGER test_suppress_delivery_update ON consumer_delivery_state");
        jdbc.execute("DROP FUNCTION test_suppress_delivery_update()");
    }

    private String stock(int quantity) {
        var sku = "SKU-" + UUID.randomUUID();
        stocks.create(sku, quantity);
        return sku;
    }

    private OrderPlaced input(String sku, int quantity) {
        return new OrderPlaced(UUID.randomUUID(), UUID.randomUUID(), List.of(new ReservationLine(UUID.randomUUID(), sku, quantity)));
    }

    private ConsumerRecord<byte[], byte[]> event(String sku, int quantity) {
        var input = input(sku, quantity);
        var json = mapper.createObjectNode();
        json.put("eventId", input.eventId().toString());
        json.put("eventType", "OrderPlaced"); json.put("eventVersion", 1); json.put("occurredAt", "2026-10-06T09:00:00Z");
        json.put("aggregateId", input.orderId().toString()); json.put("correlationId", input.orderId().toString());
        var payload = json.putObject("payload"); payload.put("orderId", input.orderId().toString()); payload.set("lines", mapper.valueToTree(input.lines()));
        return new ConsumerRecord<>("warexis.order.events.v1", 0, offset.incrementAndGet(), input.orderId().toString().getBytes(StandardCharsets.UTF_8), mapper.writeValueAsBytes(json));
    }

    @SuppressWarnings("unchecked")
    private Consumer<byte[], byte[]> consumer() { return mock(Consumer.class); }

    @SuppressWarnings("unchecked")
    private KafkaTemplate<byte[], byte[]> failingKafka() {
        KafkaTemplate<byte[], byte[]> template = mock(KafkaTemplate.class);
        when(template.send(any(org.apache.kafka.clients.producer.ProducerRecord.class))).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("synthetic broker failure")));
        return template;
    }

    private int count(String sql) { return jdbc.queryForObject(sql, Integer.class); }

    private ConsumerRecord<byte[], byte[]> consume(String topic, byte[] key) {
        return consumeCopies(topic, key, 1).getFirst();
    }

    private List<ConsumerRecord<byte[], byte[]>> consumeCopies(String topic, byte[] key, int count) {

        var configuration = Map.<String, Object>of("bootstrap.servers", broker.getBootstrapServers(), "group.id", "test-" + UUID.randomUUID(),
                "auto.offset.reset", "earliest", "enable.auto.commit", false,
                "key.deserializer", ByteArrayDeserializer.class, "value.deserializer", ByteArrayDeserializer.class);
        try (var consumer = new KafkaConsumer<byte[], byte[]>(configuration)) {
            consumer.subscribe(List.of(topic));
            var matches = new java.util.ArrayList<ConsumerRecord<byte[], byte[]>>();
            var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(100))) {
                    if (java.util.Arrays.equals(record.key(), key)) {
                        matches.add(record);
                        if (matches.size() == count) return matches;
                    }
                }
            }
            throw new AssertionError("Expected workflow record was not broker-acknowledged.");
        }
    }
}
