package io.github.panteliszara.warexis.fulfillment.messaging;

import java.util.HashMap;

import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "warexis.messaging.enabled", havingValue = "true")
@EnableConfigurationProperties(MessagingProperties.class)
@EnableKafka
@EnableScheduling
public class KafkaConfiguration {
    public static final String ORDER_TOPIC = "warexis.order.events.v1";
    public static final String FULFILLMENT_TOPIC = "warexis.fulfillment.events.v1";
    public static final String GROUP = "warexis.fulfillment.order-events.v1";
    public static final String DLT = "warexis.order.events.v1.fulfillment.dlt";

    @Bean
    DefaultKafkaProducerFactory<byte[], byte[]> workflowProducerFactory(@Value("${spring.kafka.bootstrap-servers:${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}}") String bootstrap) {
        var props = new HashMap<String, Object>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    KafkaTemplate<byte[], byte[]> workflowKafka(DefaultKafkaProducerFactory<byte[], byte[]> factory) {
        return new KafkaTemplate<>(factory);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<byte[], byte[]> kafkaListenerContainerFactory(
            @Value("${spring.kafka.bootstrap-servers:${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}}") String bootstrap,
            DeliveryState deliveries, KafkaListenerEndpointRegistry registry,
            @Value("${spring.kafka.listener.auto-startup:true}") boolean autoStartup) {
        var props = new HashMap<String, Object>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        var factory = new ConcurrentKafkaListenerContainerFactory<byte[], byte[]>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));
        factory.setConcurrency(3);
        factory.setAutoStartup(autoStartup);
        factory.setCommonErrorHandler(new org.springframework.kafka.listener.CommonContainerStoppingErrorHandler());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setAssignmentCommitOption(ContainerProperties.AssignmentCommitOption.NEVER);
        factory.getContainerProperties().setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {
            @Override
            public void onPartitionsAssigned(org.apache.kafka.clients.consumer.Consumer<?, ?> consumer,
                    java.util.Collection<org.apache.kafka.common.TopicPartition> partitions) {
                for (var partition : partitions) {
                    boolean paused = false;
                    try {
                        var offset = deliveries.pausedOffset(partition);
                        if (offset.isPresent()) {
                            consumer.seek(partition, offset.get());
                            paused = true;
                        }
                    } catch (RuntimeException ex) {
                        paused = true;
                    }
                    if (paused) {
                        consumer.pause(java.util.List.of(partition));
                        var container = registry.getListenerContainer("fulfillment-order");
                        if (container != null) {
                            container.pausePartition(partition);
                        }
                    }
                }
            }
        });
        return factory;
    }

    @Bean
    KafkaAdmin workflowKafkaAdmin(@Value("${spring.kafka.bootstrap-servers:${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}}") String bootstrap) {
        return new KafkaAdmin(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap));
    }

    @Bean
    KafkaAdmin.NewTopics workflowTopics() {
        return new KafkaAdmin.NewTopics(
                new org.apache.kafka.clients.admin.NewTopic(ORDER_TOPIC, 3, (short) 1),
                new org.apache.kafka.clients.admin.NewTopic(FULFILLMENT_TOPIC, 3, (short) 1),
                new org.apache.kafka.clients.admin.NewTopic(DLT, 3, (short) 1));
    }
}
