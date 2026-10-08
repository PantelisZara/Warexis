package io.github.panteliszara.warexis.order;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.sql.DataSource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.ObjectMapper;
import io.github.panteliszara.warexis.order.messaging.KafkaConfiguration;
import io.github.panteliszara.warexis.order.messaging.MessagingProperties;
import io.github.panteliszara.warexis.order.messaging.OutboxRelay;
import io.github.panteliszara.warexis.order.orders.application.OrderService;
import io.github.panteliszara.warexis.order.orders.application.OrderView;
import io.github.panteliszara.warexis.order.orders.domain.OrderLineInput;
import io.github.panteliszara.warexis.order.orders.domain.OrderStatus;

import static org.assertj.core.api.Assertions.*;

@org.springframework.test.annotation.DirtiesContext(classMode=org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS)
@Testcontainers
@org.junit.jupiter.api.extension.ExtendWith(org.springframework.boot.test.system.OutputCaptureExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@SpringBootTest(webEnvironment=SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties={"warexis.messaging.enabled=true","warexis.messaging.retry-base-delay=10ms","warexis.messaging.poll-interval=50ms"})
@Import(OrderServiceApplicationTests.PostgresConfiguration.class)
class KafkaWorkflowTests {
    @Container static KafkaContainer broker=new KafkaContainer("apache/kafka:4.2.2");
    @DynamicPropertySource static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers",broker::getBootstrapServers);
    }

    @Autowired OrderService orders;
    @Autowired JdbcClient jdbc;
    @Autowired ObjectMapper mapper;
    @Autowired KafkaTemplate<byte[],byte[]> kafka;
    @Autowired DataSource dataSource;
    @Autowired MessagingProperties properties;
    @Autowired PlatformTransactionManager transactions;
    @Autowired org.springframework.kafka.config.KafkaListenerEndpointRegistry registry;

    @Test @Order(1)
    void publisherSendsCommittedStoredEnvelopeAndMarksOnlyAfterBrokerAcknowledgment() throws Exception {
        var order=create();
        var persisted=jdbc.sql("SELECT event_json::text FROM outbox_events WHERE aggregate_id=:id")
                .param("id",order.id()).query(String.class).single();
        try(var consumer=consumer(KafkaConfiguration.ORDER_TOPIC)) {
            var record=find(consumer,order.id().toString().getBytes(StandardCharsets.UTF_8));
            assertThat(record.value()).isEqualTo(persisted.getBytes(StandardCharsets.UTF_8));
        }
        await(()->published(order));
        assertThat(jdbc.sql("SELECT publication_attempts FROM outbox_events WHERE aggregate_id=:id")
                .param("id",order.id()).query(Integer.class).single()).isEqualTo(1);
    }

    @Test @Order(2)
    void listenerCommitsSuccessfulAndRejectedDecisionsAndHandlesDuplicateDelivery() throws Exception {
        var confirmed=create();
        var json=outcome(confirmed,"InventoryReserved",null);
        var sent=kafka.send(KafkaConfiguration.INVENTORY_TOPIC,bytes(confirmed.id().toString()),bytes(json)).get(10,TimeUnit.SECONDS);
        await(()->orders.get(confirmed.id()).status()==OrderStatus.CONFIRMED);
        var terminal=orders.get(confirmed.id());
        var duplicate=kafka.send(KafkaConfiguration.INVENTORY_TOPIC,bytes(confirmed.id().toString()),bytes(json)).get(10,TimeUnit.SECONDS);
        awaitComplete(duplicate.getRecordMetadata());
        assertThat(orders.get(confirmed.id())).isEqualTo(terminal);
        assertThat(jdbc.sql("SELECT count(*) FROM processed_inventory_events WHERE order_id=:id").param("id",confirmed.id()).query(Long.class).single()).isEqualTo(1);
        var rejected=create();
        var rejection=outcome(rejected,"InventoryRejected","UNKNOWN_SKU");
        kafka.send(KafkaConfiguration.INVENTORY_TOPIC,bytes(rejected.id().toString()),bytes(rejection)).get(10,TimeUnit.SECONDS);
        await(()->orders.get(rejected.id()).status()==OrderStatus.REJECTED);
        var rejectedTerminal=orders.get(rejected.id());
        var repeated=kafka.send(KafkaConfiguration.INVENTORY_TOPIC,bytes(rejected.id().toString()),bytes(rejection)).get(10,TimeUnit.SECONDS);
        awaitComplete(repeated.getRecordMetadata());
        assertThat(orders.get(rejected.id())).isEqualTo(rejectedTerminal);
        assertThat(jdbc.sql("SELECT count(*) FROM inventory_decisions WHERE order_id=:id").param("id",rejected.id()).query(Long.class).single()).isEqualTo(1);
        assertThat(jdbc.sql("SELECT count(*) FROM processed_inventory_events WHERE order_id=:id").param("id",rejected.id()).query(Long.class).single()).isEqualTo(1);
    }

    @Test @Order(3)
    void poisonInvalidUtf8RetainsExactBytesKeyAndPartitionInConfirmedDlt() throws Exception {
        byte[] key={(byte)0xff};
        byte[] poison={(byte)0xc3,0x28};
        var sent=kafka.send(new ProducerRecord<byte[],byte[]>(KafkaConfiguration.INVENTORY_TOPIC,1,key,poison)).get(10,TimeUnit.SECONDS);
        try(var consumer=consumer(KafkaConfiguration.DLT)) {
            var record=find(consumer,key);
            assertThat(record.value()).isEqualTo(poison);
            assertThat(record.partition()).isEqualTo(1);
            assertThat(new String(record.headers().lastHeader("warexis-source-topic").value(),StandardCharsets.UTF_8)).isEqualTo(KafkaConfiguration.INVENTORY_TOPIC);
            assertThat(new String(record.headers().lastHeader("warexis-source-offset").value(),StandardCharsets.UTF_8)).isEqualTo(Long.toString(sent.getRecordMetadata().offset()));
            assertThat(new String(record.headers().lastHeader("warexis-consumer-group").value(),StandardCharsets.UTF_8)).isEqualTo(KafkaConfiguration.GROUP);
            assertThat(new String(record.headers().lastHeader("warexis-processing-attempts").value(),StandardCharsets.UTF_8)).isEqualTo("1");
        }
        await(()->jdbc.sql("SELECT phase FROM messaging_deliveries WHERE source_topic=:topic AND source_partition=1 AND source_offset=:offset")
                .param("topic",KafkaConfiguration.INVENTORY_TOPIC).param("offset",sent.getRecordMetadata().offset()).query(String.class).single().equals("COMPLETE"));
    }

    @Test @Order(4)
    void exhaustedDatabaseFailureQuarantinesAfterFourAttemptsAndRollsBackBusinessState() throws Exception {
        var order=create();
        var json=outcome(order,"InventoryReserved",null);
        jdbc.sql("ALTER TABLE processed_inventory_events ADD CONSTRAINT reject_processed_event CHECK(FALSE) NOT VALID").update();
        try {
            var sent=kafka.send(KafkaConfiguration.INVENTORY_TOPIC,bytes(order.id().toString()),bytes(json)).get(10,TimeUnit.SECONDS);
            try(var consumer=consumer(KafkaConfiguration.DLT)) {
                assertThat(find(consumer,bytes(order.id().toString())).value()).isEqualTo(bytes(json));
            }
            await(()->jdbc.sql("SELECT phase FROM messaging_deliveries WHERE source_topic=:topic AND source_partition=:partition AND source_offset=:offset")
                    .param("topic",KafkaConfiguration.INVENTORY_TOPIC).param("partition",sent.getRecordMetadata().partition())
                    .param("offset",sent.getRecordMetadata().offset()).query(String.class).single().equals("COMPLETE"));
            assertThat(jdbc.sql("SELECT processing_attempts FROM messaging_deliveries WHERE source_topic=:topic AND source_partition=:partition AND source_offset=:offset")
                    .param("topic",KafkaConfiguration.INVENTORY_TOPIC).param("partition",sent.getRecordMetadata().partition())
                    .param("offset",sent.getRecordMetadata().offset()).query(Integer.class).single()).isEqualTo(4);
            assertThat(orders.get(order.id()).status()).isEqualTo(OrderStatus.PENDING_INVENTORY);
            assertThat(jdbc.sql("SELECT count(*) FROM inventory_decisions WHERE order_id=:id").param("id",order.id()).query(Long.class).single()).isZero();
        } finally {
            jdbc.sql("ALTER TABLE processed_inventory_events DROP CONSTRAINT reject_processed_event").update();
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings={"OrderPlaced","OrderConfirmed"})
    @Order(5)
    void acknowledgmentThenDatabaseFailureReplaysIdenticalEventAndDurablyPausesAtFiveAttempts(String type, org.springframework.boot.test.system.CapturedOutput output) throws Exception {
        await(()->jdbc.sql("SELECT count(*) FROM outbox_events WHERE published_at IS NULL").query(Long.class).single()==0);
        jdbc.sql("ALTER TABLE outbox_events ADD CONSTRAINT reject_publication_mark CHECK(event_type<>'"+type+"' OR published_at IS NULL) NOT VALID").update();
        var order=create();
        if(type.equals("OrderConfirmed")) {
            kafka.send(KafkaConfiguration.INVENTORY_TOPIC,bytes(order.id().toString()),bytes(outcome(order,"InventoryReserved",null))).get(10,TimeUnit.SECONDS);
            await(()->orders.get(order.id()).status()==OrderStatus.CONFIRMED);
        }
        var stored=jdbc.sql("SELECT event_json::text FROM outbox_events WHERE aggregate_id=:id AND event_type=:type").param("id",order.id()).param("type",type).query(String.class).single();
        try {
            await(()->jdbc.sql("SELECT paused FROM messaging_relay_control WHERE id=1").query(Boolean.class).single());
            assertThat(jdbc.sql("SELECT publication_attempts FROM outbox_events WHERE aggregate_id=:id AND event_type=:type").param("id",order.id()).param("type",type).query(Integer.class).single()).isEqualTo(5);
            assertThat(published(order,type)).isFalse();
            assertThat(output.getOut()).contains("state=PAUSED, attempt=5, classification=RETRY_EXHAUSTED");
            try(var consumer=consumer(KafkaConfiguration.ORDER_TOPIC)) {
                int repeats=0;
                var deadline=Instant.now().plusSeconds(15);
                while(repeats<5 && Instant.now().isBefore(deadline)) {
                    for(var record:consumer.poll(Duration.ofMillis(200))) {
                        if(java.util.Arrays.equals(record.key(),bytes(order.id().toString()))
                                && mapper.readTree(new String(record.value(),StandardCharsets.UTF_8)).path("eventType").asString().equals(type)) {
                            assertThat(record.value()).isEqualTo(bytes(stored));
                            repeats++;
                        }
                    }
                }
                assertThat(repeats).isEqualTo(5);
            }
            new OutboxRelay(dataSource,jdbc,kafka,properties,transactions).publishNext();
            assertThat(jdbc.sql("SELECT publication_attempts FROM outbox_events WHERE aggregate_id=:id AND event_type=:type").param("id",order.id()).param("type",type).query(Integer.class).single()).isEqualTo(5);
        } finally {
            jdbc.sql("ALTER TABLE outbox_events DROP CONSTRAINT reject_publication_mark").update();
            jdbc.sql("UPDATE outbox_events SET publication_attempts=0,next_attempt_at=NULL WHERE aggregate_id=:id AND event_type=:type").param("id",order.id()).param("type",type).update();
            jdbc.sql("UPDATE messaging_relay_control SET paused=FALSE,failure_classification=NULL WHERE id=1").update();
        }
        await(()->published(order,type));
    }

    @Test @Order(6)
    void realPartitionPausePreventsLaterRecordAcrossPollsAndRestoresDurablyOnRestart() throws Exception {
        var container=registry.getListenerContainer("order-inventory");
        var partition=new org.apache.kafka.common.TopicPartition(KafkaConfiguration.INVENTORY_TOPIC,2);
        container.stop();
        container.getContainerProperties().setPollTimeout(100);
        var first=create();
        var later=create();
        var blocked=kafka.send(new ProducerRecord<byte[],byte[]>(KafkaConfiguration.INVENTORY_TOPIC,2,
                bytes(first.id().toString()),bytes(outcome(first,"InventoryReserved",null))))
                .get(10,TimeUnit.SECONDS).getRecordMetadata();
        kafka.send(new ProducerRecord<byte[],byte[]>(KafkaConfiguration.INVENTORY_TOPIC,2,
                bytes(later.id().toString()),bytes(outcome(later,"InventoryReserved",null)))).get(10,TimeUnit.SECONDS);
        jdbc.sql("ALTER TABLE messaging_deliveries ADD CONSTRAINT reject_real_completion CHECK(source_partition<>2 OR source_offset<>"
                + blocked.offset()+" OR phase<>'COMPLETE') NOT VALID").update();
        try {
            container.start();
            await(()->container.isPartitionPaused(partition));
            assertThat(orders.get(first.id()).status()).isEqualTo(OrderStatus.CONFIRMED);
            var other=create();
            kafka.send(new ProducerRecord<byte[],byte[]>(KafkaConfiguration.INVENTORY_TOPIC,0,
                    bytes(other.id().toString()),bytes(outcome(other,"InventoryReserved",null)))).get(10,TimeUnit.SECONDS);
            await(()->orders.get(other.id()).status()==OrderStatus.CONFIRMED);
            assertPartitionBlocked(later,blocked);
            container.stop();
            jdbc.sql("ALTER TABLE messaging_deliveries DROP CONSTRAINT reject_real_completion").update();
            jdbc.sql("UPDATE messaging_deliveries SET phase='PAUSED' WHERE source_topic=:topic AND source_partition=2 AND source_offset=:offset")
                    .param("topic",KafkaConfiguration.INVENTORY_TOPIC).param("offset",blocked.offset()).update();
            container.resumePartition(partition);
            container.start();
            await(()->container.getAssignedPartitions().contains(partition));
            // Rebalance restores the raw consumer pause before Spring updates child assignment bookkeeping.
            // Assert externally observable processing/offset behavior, not its later internal pause flag.
            assertPartitionBlocked(later,blocked);
            assertThat(jdbc.sql("SELECT processing_attempts FROM messaging_deliveries WHERE source_topic=:topic AND source_partition=2 AND source_offset=:offset")
                    .param("topic",KafkaConfiguration.INVENTORY_TOPIC).param("offset",blocked.offset()).query(Integer.class).single()).isEqualTo(1);
        } finally {
            container.stop();
            jdbc.sql("ALTER TABLE messaging_deliveries DROP CONSTRAINT IF EXISTS reject_real_completion").update();
            jdbc.sql("UPDATE messaging_deliveries SET phase='PROCESS',next_attempt_at=NULL WHERE source_topic=:topic AND source_partition=2 AND source_offset=:offset")
                    .param("topic",KafkaConfiguration.INVENTORY_TOPIC).param("offset",blocked.offset()).update();
            container.resumePartition(partition);
            container.start();
        }
        await(()->orders.get(later.id()).status()==OrderStatus.CONFIRMED);
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings={"CONFIRMED","REJECTED","UNKNOWN"})
    @Order(7)
    void conflictingTerminalAndUnknownOrderOutcomesAreQuarantinedWithoutRetryOrMutation(String initial, org.springframework.boot.test.system.CapturedOutput output) throws Exception {
        var order=create();
        var source=outcome(order,initial.equals("REJECTED")?"InventoryRejected":"InventoryReserved",
                initial.equals("REJECTED")?"INSUFFICIENT_STOCK":null);
        if(!initial.equals("UNKNOWN")) {
            kafka.send(KafkaConfiguration.INVENTORY_TOPIC,bytes(order.id().toString()),bytes(source)).get(10,TimeUnit.SECONDS);
            await(()->orders.get(order.id()).status().name().equals(initial));
        }
        var terminal=orders.get(order.id());
        var incoming=(tools.jackson.databind.node.ObjectNode)mapper.readTree(source);
        incoming.put("eventId",UUID.randomUUID().toString());
        incoming.put("untrustedDiagnostic","UNSAFE_PAYLOAD_MARKER\nFORGED_LOG_LINE");
        var id=order.id();
        if(initial.equals("UNKNOWN")) {
            id=UUID.randomUUID();
            incoming.put("correlationId",id.toString());
            ((tools.jackson.databind.node.ObjectNode)incoming.path("payload")).put("orderId",id.toString());
        } else if(initial.equals("CONFIRMED")) {
            incoming.put("eventType","InventoryRejected");
            ((tools.jackson.databind.node.ObjectNode)incoming.path("payload")).put("reasonCode","INSUFFICIENT_STOCK");
        } else {
            incoming.put("eventType","InventoryReserved");
            ((tools.jackson.databind.node.ObjectNode)incoming.path("payload")).remove("reasonCode");
        }
        var body=bytes(mapper.writeValueAsString(incoming));
        var key=bytes(id.toString());
        var record=kafka.send(new ProducerRecord<byte[],byte[]>(KafkaConfiguration.INVENTORY_TOPIC,1,key,body))
                .get(10,TimeUnit.SECONDS).getRecordMetadata();
        try(var consumer=consumer(KafkaConfiguration.DLT)) {
            var dlt=find(consumer,key);
            assertThat(dlt.value()).isEqualTo(body);
            assertThat(dlt.partition()).isEqualTo(record.partition());
            assertThat(new String(dlt.headers().lastHeader("warexis-source-offset").value(),StandardCharsets.UTF_8)).isEqualTo(Long.toString(record.offset()));
            assertThat(new String(dlt.headers().lastHeader("warexis-processing-attempts").value(),StandardCharsets.UTF_8)).isEqualTo("1");
            assertThat(new String(dlt.headers().lastHeader("warexis-failure-code").value(),StandardCharsets.UTF_8)).isEqualTo("INVALID_OR_CONFLICTING");
        }
        awaitComplete(record);
        assertThat(output.getOut()).contains("eventId="+incoming.path("eventId").asString(),
                "eventType="+incoming.path("eventType").asString(),"orderId="+id,"state=DLT", "attempt=1");
        assertThat(output.getAll()).doesNotContain("UNSAFE_PAYLOAD_MARKER","FORGED_LOG_LINE");
        try(var admin=org.apache.kafka.clients.admin.AdminClient.create(Map.of("bootstrap.servers",broker.getBootstrapServers()))) {
            await(()->{
                try {
                    var offset=admin.listConsumerGroupOffsets(KafkaConfiguration.GROUP).partitionsToOffsetAndMetadata()
                            .get(5,TimeUnit.SECONDS).get(new org.apache.kafka.common.TopicPartition(record.topic(),record.partition()));
                    return offset!=null && offset.offset()>record.offset();
                } catch(Exception ex) { return false; }
            });
        }
        assertThat(orders.get(order.id())).isEqualTo(terminal);
        assertThat(jdbc.sql("SELECT count(*) FROM processed_inventory_events WHERE event_id=:id")
                .param("id",UUID.fromString(incoming.path("eventId").asString())).query(Long.class).single()).isZero();
        assertThat(jdbc.sql("SELECT count(*) FROM inventory_decisions WHERE order_id=:id")
                .param("id",order.id()).query(Long.class).single()).isEqualTo(initial.equals("UNKNOWN")?0:1);
    }

    private void awaitComplete(org.apache.kafka.clients.producer.RecordMetadata source) throws Exception {
        await(()->jdbc.sql("SELECT count(*) FROM messaging_deliveries WHERE source_topic=:topic AND source_partition=:partition AND source_offset=:offset AND phase='COMPLETE'")
                .param("topic",source.topic()).param("partition",source.partition()).param("offset",source.offset()).query(Long.class).single()==1);
    }

    private void assertPartitionBlocked(OrderView later,org.apache.kafka.clients.producer.RecordMetadata offending) throws Exception {
        // The configured 100ms poll interval allows repeated real polls while the partition stays paused.
        Thread.sleep(1200);
        assertThat(orders.get(later.id()).status()).isEqualTo(OrderStatus.PENDING_INVENTORY);
        assertThat(jdbc.sql("SELECT count(*) FROM processed_inventory_events WHERE order_id=:id")
                .param("id",later.id()).query(Long.class).single()).isZero();
        try(var admin=org.apache.kafka.clients.admin.AdminClient.create(Map.of("bootstrap.servers",broker.getBootstrapServers()))) {
            var offset=admin.listConsumerGroupOffsets(KafkaConfiguration.GROUP).partitionsToOffsetAndMetadata()
                    .get(10,TimeUnit.SECONDS).get(new org.apache.kafka.common.TopicPartition(offending.topic(),offending.partition()));
            assertThat(offset==null || offset.offset()<=offending.offset()).isTrue();
        }
    }

    private KafkaConsumer<byte[],byte[]> consumer(String topic) {
        var consumer=new KafkaConsumer<byte[],byte[]>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,broker.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG,"test-"+UUID.randomUUID(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,ByteArrayDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,ByteArrayDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest",ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false));
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    private ConsumerRecord<byte[],byte[]> find(KafkaConsumer<byte[],byte[]> consumer,byte[] key) {
        var deadline=Instant.now().plusSeconds(20);
        while(Instant.now().isBefore(deadline)) {
            for(var record:consumer.poll(Duration.ofMillis(200))) {
                if(java.util.Arrays.equals(record.key(),key)) return record;
            }
        }
        throw new AssertionError("Expected workflow record was not received.");
    }

    private void await(BooleanSupplier condition) throws Exception {
        var deadline=Instant.now().plusSeconds(25);
        while(Instant.now().isBefore(deadline)) {
            if(condition.getAsBoolean()) return;
            Thread.sleep(25);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }

    private boolean published(OrderView order) {
        return published(order,"OrderPlaced");
    }

    private boolean published(OrderView order,String type) {
        return jdbc.sql("SELECT published_at IS NOT NULL FROM outbox_events WHERE aggregate_id=:id AND event_type=:type")
                .param("id",order.id()).param("type",type).query(Boolean.class).single();
    }

    private OrderView create() {
        return orders.create(List.of(new OrderLineInput("A",2),new OrderLineInput("A",3)));
    }

    private String outcome(OrderView order,String type,String reason) {
        var event=mapper.createObjectNode();
        event.put("eventId",UUID.randomUUID().toString());event.put("eventType",type);event.put("eventVersion",1);
        event.put("occurredAt",Instant.now().toString());
        var reservation=UUID.randomUUID().toString();
        event.put("aggregateId",reservation);event.put("correlationId",order.id().toString());
        event.put("causationId",jdbc.sql("SELECT event_id FROM outbox_events WHERE aggregate_id=:id").param("id",order.id()).query(UUID.class).single().toString());
        var payload=event.putObject("payload");payload.put("orderId",order.id().toString());payload.put("reservationId",reservation);
        var lines=payload.putArray("lines");
        for(var line:order.lines()) {
            var node=lines.addObject();node.put("orderLineId",line.id().toString());node.put("sku",line.sku());node.put("quantity",line.quantity());
        }
        if(reason!=null) payload.put("reasonCode",reason);
        return mapper.writeValueAsString(event);
    }

    private byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
}
