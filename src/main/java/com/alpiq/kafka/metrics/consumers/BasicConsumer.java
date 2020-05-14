package com.alpiq.kafka.metrics.consumers;

import com.alpiq.kafka.metrics.config.ConsumerConfiguration;
import com.alpiq.kafka.metrics.dto.ConsumerConfigurationDTO;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static java.lang.System.exit;

/**
 * BasicConsumer allows to consume the records from a Kafka topic defined in the config.properties file
 * found in the resources directory of this project.
 */
public class BasicConsumer {
    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(BasicConsumer.class.getName());

        try {
            // Read the Kafka configuration for the consumer
            ConsumerConfiguration consumerConfiguration = new ConsumerConfiguration();
            ConsumerConfigurationDTO consumerConfigurationDTO = consumerConfiguration.getConsumerConfiguration();

            String bootstrapServers = consumerConfigurationDTO.getBootstrapServers();
            String groupId = consumerConfigurationDTO.getGroupId();
            String topicName = consumerConfigurationDTO.getTopicName();

            // Create the consumer configuration
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

            // Create the consumer
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

            // Subscribe the consumer to our topic
            consumer.subscribe(Collections.singleton(topicName));

            // Poll for new events
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord record : records) {
                    logger.info("key : "+ record.key() + ", value "+ record.value());
                    logger.info("Partition: " + record.partition() + ", offset: " + record.offset());
                }
            }
        } catch (Exception e) {
            exit(1);
        }
    }
}
