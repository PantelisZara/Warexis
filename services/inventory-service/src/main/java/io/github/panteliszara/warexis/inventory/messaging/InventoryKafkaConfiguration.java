package io.github.panteliszara.warexis.inventory.messaging;

import java.util.Collection;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.CommonContainerStoppingErrorHandler;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(MessagingProperties.class)
public class InventoryKafkaConfiguration {
    private static final Logger log = LoggerFactory.getLogger(InventoryKafkaConfiguration.class);

    @Bean
    @ConditionalOnProperty(name = "warexis.messaging.enabled", havingValue = "true")
    NewTopic orderEvents() { return TopicBuilder.name("warexis.order.events.v1").partitions(3).replicas(1).build(); }

    @Bean
    @ConditionalOnProperty(name = "warexis.messaging.enabled", havingValue = "true")
    NewTopic inventoryEvents() { return TopicBuilder.name("warexis.inventory.events.v1").partitions(3).replicas(1).build(); }

    @Bean
    @ConditionalOnProperty(name = "warexis.messaging.enabled", havingValue = "true")
    NewTopic inventoryDlt() { return TopicBuilder.name(InventoryOrderListener.DLT).partitions(3).replicas(1).build(); }

    @Bean
    @ConditionalOnProperty(name = "warexis.messaging.enabled", havingValue = "true")
    ConcurrentKafkaListenerContainerFactory<byte[], byte[]> inventoryKafkaListenerContainerFactory(
            ConsumerFactory<byte[], byte[]> consumers, ConsumerDeliveryStore delivery,
            @Value("${spring.kafka.listener.auto-startup:true}") boolean autoStartup) {
        var factory = new ConcurrentKafkaListenerContainerFactory<byte[], byte[]>();
        factory.setConsumerFactory(consumers);
        factory.setConcurrency(3);
        factory.setAutoStartup(autoStartup);
        factory.getContainerProperties().setObservationEnabled(false);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setAssignmentCommitOption(ContainerProperties.AssignmentCommitOption.NEVER);
        factory.setCommonErrorHandler(new CommonContainerStoppingErrorHandler());
        factory.getContainerProperties().setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                try {
                    for (var partition : partitions) {
                        var offset = delivery.pausedOffset(partition);
                        if (offset.isPresent()) {
                            consumer.seek(partition, offset.getAsLong());
                            consumer.pause(java.util.Set.of(partition));
                        }
                    }
                } catch (RuntimeException exception) {
                    consumer.pause(partitions);
                    log.error("Inventory consumer paused: delivery metadata unavailable during assignment.");
                }
            }
        });
        return factory;
    }
}
