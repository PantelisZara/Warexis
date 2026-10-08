package io.github.panteliszara.warexis.fulfillment;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.*;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.*;
import io.github.panteliszara.warexis.fulfillment.application.*;
import io.github.panteliszara.warexis.fulfillment.messaging.*;
import static org.assertj.core.api.Assertions.*;
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@org.springframework.test.annotation.DirtiesContext(classMode=org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment=SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties={"warexis.messaging.enabled=true","warexis.messaging.retry-base-delay=10ms","warexis.messaging.poll-interval=50ms"})
@Import(FulfillmentServiceTests.PostgresConfiguration.class)
class KafkaWorkflowTests {
    @Container static KafkaContainer broker=new KafkaContainer("apache/kafka:4.2.2");
    @DynamicPropertySource static void kafka(DynamicPropertyRegistry r) { r.add("spring.kafka.bootstrap-servers",broker::getBootstrapServers); }
    @Autowired FulfillmentService service;
    @Autowired JdbcClient jdbc;
    @Autowired KafkaTemplate<byte[],byte[]> kafka;
    @Autowired org.springframework.kafka.config.KafkaListenerEndpointRegistry registry;
    @Autowired javax.sql.DataSource dataSource;
    @Autowired MessagingProperties properties;
    @Autowired org.springframework.transaction.PlatformTransactionManager transactions;
    @Test @Order(1) void confirmationCreatesPendingAndDuplicateKafkaDeliveryRemainsIdempotent() throws Exception {
        var in=FulfillmentServiceTests.input();var body=EventJsonTests.envelope(in);
        var sent=send(in.orderId(),body);awaitComplete(sent);
        var plan=service.byOrder(in.orderId());assertThat(plan.status()).isEqualTo("PENDING");
        var repeat=send(in.orderId(),body);awaitComplete(repeat);
        assertThat(service.byOrder(in.orderId())).isEqualTo(plan);
        assertThat(jdbc.sql("SELECT COUNT(*) FROM processed_confirmations WHERE fulfillment_id=:id").param("id",plan.id()).query(Long.class).single()).isEqualTo(1);
    }
    @Test @Order(2) void validatedOrderPlacedCompletesWithoutCreatingFulfillment() throws Exception {
        var in=FulfillmentServiceTests.input();var root=(tools.jackson.databind.node.ObjectNode)tools.jackson.databind.json.JsonMapper.builder().build().readTree(EventJsonTests.envelope(in));
        root.put("eventType","OrderPlaced");root.remove("causationId");((tools.jackson.databind.node.ObjectNode)root.path("payload")).remove("reservationId");
        var sent=send(in.orderId(),root.toString());awaitComplete(sent);
        assertThat(jdbc.sql("SELECT COUNT(*) FROM fulfillments WHERE order_id=:id").param("id",in.orderId()).query(Long.class).single()).isZero();
        assertThat(jdbc.sql("SELECT COUNT(*) FROM processed_confirmations WHERE event_id=:id").param("id",in.eventId()).query(Long.class).single()).isZero();
        assertThat(delivery(sent,"processing_attempts")).isZero();
        assertThat(delivery(sent,"dlt_attempts")).isZero();
    }
    @Test @Order(3) void poisonDltPreservesExactBytesSourcePartitionAndOffsets() throws Exception {
        byte[] key={(byte)0xff};byte[] value={(byte)0xc3,0x28};
        var sent=kafka.send(new ProducerRecord<byte[],byte[]>(KafkaConfiguration.ORDER_TOPIC,1,key,value)).get(10,TimeUnit.SECONDS).getRecordMetadata();
        try(var observer=consumer(KafkaConfiguration.DLT)) {
            var record=find(observer,key);assertThat(record.value()).isEqualTo(value);assertThat(record.partition()).isEqualTo(1);
            assertThat(new String(record.headers().lastHeader("warexis-source-topic").value(),StandardCharsets.UTF_8)).isEqualTo(KafkaConfiguration.ORDER_TOPIC);
            assertThat(new String(record.headers().lastHeader("warexis-source-offset").value(),StandardCharsets.UTF_8)).isEqualTo(Long.toString(sent.offset()));
            assertThat(new String(record.headers().lastHeader("warexis-consumer-group").value(),StandardCharsets.UTF_8)).isEqualTo(KafkaConfiguration.GROUP);
        }
        awaitComplete(sent);
        assertThat(delivery(sent,"processing_attempts")).isEqualTo(1);
        assertThat(delivery(sent,"dlt_attempts")).isEqualTo(1);
    }
    @Test @Order(4) void databaseRollbackQuarantinesOnlyAfterFourPersistedAttempts() throws Exception {
        var in=FulfillmentServiceTests.input();var body=EventJsonTests.envelope(in);
        jdbc.sql("ALTER TABLE processed_confirmations ADD CONSTRAINT reject_input CHECK(FALSE) NOT VALID").update();
        try {
            var sent=send(in.orderId(),body);
            try(var observer=consumer(KafkaConfiguration.DLT)) { assertThat(find(observer,bytes(in.orderId().toString())).value()).isEqualTo(bytes(body)); }
            awaitComplete(sent);assertThat(delivery(sent,"processing_attempts")).isEqualTo(4);
            assertThat(jdbc.sql("SELECT COUNT(*) FROM fulfillments WHERE order_id=:id").param("id",in.orderId()).query(Long.class).single()).isZero();
        } finally { jdbc.sql("ALTER TABLE processed_confirmations DROP CONSTRAINT reject_input").update(); }
    }
    @Test @Order(5) void manualDispatchPublishesExactStoredFactThenMarksAfterBrokerAck() throws Exception {
        var in=FulfillmentServiceTests.input();service.accept(in);var plan=service.byOrder(in.orderId());service.ship(plan.id(),"KAFKA-"+UUID.randomUUID());
        var stored=jdbc.sql("SELECT event_json::text FROM outbox_events WHERE aggregate_id=:id").param("id",plan.id()).query(String.class).single();
        try(var observer=consumer(KafkaConfiguration.FULFILLMENT_TOPIC)) {
            assertThat(find(observer,bytes(in.orderId().toString())).value()).isEqualTo(bytes(stored));
        }
        await(()->jdbc.sql("SELECT published_at IS NOT NULL FROM outbox_events WHERE aggregate_id=:id").param("id",plan.id()).query(Boolean.class).single());
        assertThat(jdbc.sql("SELECT publication_attempts FROM outbox_events WHERE aggregate_id=:id").param("id",plan.id()).query(Integer.class).single()).isEqualTo(1);
    }
    @Test @Order(6) void acknowledgedFactWithFailedCheckpointReplaysStableIdentityAndExhaustsAtFive() throws Exception {
        await(()->jdbc.sql("SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL").query(Long.class).single()==0);
        jdbc.sql("ALTER TABLE outbox_events ADD CONSTRAINT reject_publication CHECK(published_at IS NULL) NOT VALID").update();
        var in=FulfillmentServiceTests.input();service.accept(in);var plan=service.byOrder(in.orderId());service.ship(plan.id(),"REPLAY-"+UUID.randomUUID());
        var stored=jdbc.sql("SELECT event_json::text FROM outbox_events WHERE aggregate_id=:id").param("id",plan.id()).query(String.class).single();
        try {
            await(()->jdbc.sql("SELECT paused FROM messaging_relay_control WHERE id=1").query(Boolean.class).single());
            assertThat(jdbc.sql("SELECT publication_attempts FROM outbox_events WHERE aggregate_id=:id").param("id",plan.id()).query(Integer.class).single()).isEqualTo(5);
            try(var observer=consumer(KafkaConfiguration.FULFILLMENT_TOPIC)) {
                int count=0;var deadline=Instant.now().plusSeconds(20);
                while(count<5 && Instant.now().isBefore(deadline)) for(var record:observer.poll(Duration.ofMillis(100))) if(Arrays.equals(record.key(),bytes(in.orderId().toString()))) {
                    assertThat(record.value()).isEqualTo(bytes(stored));count++;
                }
                assertThat(count).isEqualTo(5);
            }
            new OutboxRelay(dataSource,jdbc,kafka,properties,transactions).publishNext();
            assertThat(jdbc.sql("SELECT publication_attempts FROM outbox_events WHERE aggregate_id=:id").param("id",plan.id()).query(Integer.class).single()).isEqualTo(5);
        } finally {
            jdbc.sql("ALTER TABLE outbox_events DROP CONSTRAINT reject_publication").update();
            jdbc.sql("UPDATE outbox_events SET publication_attempts=0,next_attempt_at=NULL WHERE aggregate_id=:id").param("id",plan.id()).update();
            jdbc.sql("UPDATE messaging_relay_control SET paused=false,failure_classification=NULL WHERE id=1").update();
            await(()->jdbc.sql("SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL").query(Long.class).single()==0);
        }
    }
    @Test @Order(7) void independentRelayInstancesRespectDedicatedSessionExclusion() throws Exception {
        try(var connection=dataSource.getConnection();var statement=connection.createStatement()) {
            statement.execute("SELECT pg_advisory_lock(96,1)");
            var in=FulfillmentServiceTests.input();service.accept(in);var plan=service.byOrder(in.orderId());service.ship(plan.id(),"LOCK-"+UUID.randomUUID());
            new OutboxRelay(dataSource,jdbc,kafka,properties,transactions).publishNext();
            assertThat(jdbc.sql("SELECT publication_attempts FROM outbox_events WHERE aggregate_id=:id").param("id",plan.id()).query(Integer.class).single()).isZero();
            statement.execute("SELECT pg_advisory_unlock(96,1)");
            await(()->jdbc.sql("SELECT published_at IS NOT NULL FROM outbox_events WHERE aggregate_id=:id").param("id",plan.id()).query(Boolean.class).single());
        }
    }
    @Test @Order(8) void pausedPartitionIsRestoredAfterConsumerContainerRestartWhileOtherPartitionContinues() throws Exception {
        var source=new org.apache.kafka.common.TopicPartition(KafkaConfiguration.ORDER_TOPIC,0);
        try(var admin=org.apache.kafka.clients.admin.AdminClient.create(Map.of("bootstrap.servers",broker.getBootstrapServers()))) {
            long offset=admin.listOffsets(Map.of(source,org.apache.kafka.clients.admin.OffsetSpec.latest())).all().get(10,TimeUnit.SECONDS).get(source).offset();
            jdbc.sql("INSERT INTO messaging_deliveries(source_topic,source_partition,source_offset,phase,dlt_attempts) VALUES(:topic,0,:offset,'PAUSED',4)")
                    .param("topic",source.topic()).param("offset",offset).update();
            var container=registry.getListenerContainer("fulfillment-order");container.stop();container.start();
            await(()->container.getAssignedPartitions()!=null && container.getAssignedPartitions().size()==3);
            var blocked=FulfillmentServiceTests.input();
            var sent=kafka.send(new ProducerRecord<byte[],byte[]>(source.topic(),0,bytes(blocked.orderId().toString()),bytes(EventJsonTests.envelope(blocked))))
                    .get(10,TimeUnit.SECONDS).getRecordMetadata();assertThat(sent.offset()).isEqualTo(offset);
            for(int round=0;round<2;round++) {
                var in=FulfillmentServiceTests.input();var progressed=kafka.send(new ProducerRecord<byte[],byte[]>(source.topic(),2,bytes(in.orderId().toString()),bytes(EventJsonTests.envelope(in))))
                        .get(10,TimeUnit.SECONDS).getRecordMetadata();awaitComplete(progressed);
                assertThat(jdbc.sql("SELECT COUNT(*) FROM fulfillments WHERE order_id=:id").param("id",blocked.orderId()).query(Long.class).single()).isZero();
                var committed=admin.listConsumerGroupOffsets(KafkaConfiguration.GROUP).partitionsToOffsetAndMetadata().get(10,TimeUnit.SECONDS).get(source);
                assertThat(committed==null || committed.offset()<=offset).isTrue();
                container.stop();container.start();await(()->container.getAssignedPartitions()!=null && container.getAssignedPartitions().size()==3);
            }
        }
    }

    private org.apache.kafka.clients.producer.RecordMetadata send(UUID order,String body) throws Exception {
        return kafka.send(KafkaConfiguration.ORDER_TOPIC,bytes(order.toString()),bytes(body)).get(10,TimeUnit.SECONDS).getRecordMetadata();
    }
    private int delivery(org.apache.kafka.clients.producer.RecordMetadata sent,String field) {
        return jdbc.sql("SELECT "+field+" FROM messaging_deliveries WHERE source_topic=:topic AND source_partition=:part AND source_offset=:offset")
                .param("topic",sent.topic()).param("part",sent.partition()).param("offset",sent.offset()).query(Integer.class).single();
    }
    private void awaitComplete(org.apache.kafka.clients.producer.RecordMetadata sent) {
        await(()->jdbc.sql("SELECT phase FROM messaging_deliveries WHERE source_topic=:topic AND source_partition=:part AND source_offset=:offset")
                .param("topic",sent.topic()).param("part",sent.partition()).param("offset",sent.offset()).query(String.class).optional().orElse("").equals("COMPLETE"));
    }
    private KafkaConsumer<byte[],byte[]> consumer(String topic) {
        var props=new HashMap<String,Object>();props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,broker.getBootstrapServers());props.put(ConsumerConfig.GROUP_ID_CONFIG,"observer-"+UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,ByteArrayDeserializer.class);props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,ByteArrayDeserializer.class);
        var consumer=new KafkaConsumer<byte[],byte[]>(props);consumer.subscribe(List.of(topic));return consumer;
    }
    private ConsumerRecord<byte[],byte[]> find(KafkaConsumer<byte[],byte[]> observer,byte[] key) {
        var deadline=Instant.now().plusSeconds(30);while(Instant.now().isBefore(deadline)) for(var record:observer.poll(Duration.ofMillis(100))) if(Arrays.equals(record.key(),key)) return record;
        throw new AssertionError("Expected broker-confirmed record not found.");
    }
    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    private static void await(BooleanSupplier condition) {
        var deadline=Instant.now().plusSeconds(35);while(Instant.now().isBefore(deadline)) { if(condition.getAsBoolean()) return;try {Thread.sleep(100);} catch(InterruptedException ex) {Thread.currentThread().interrupt();throw new AssertionError(ex);} }
        throw new AssertionError("Timed out waiting for committed state.");
    }
}
