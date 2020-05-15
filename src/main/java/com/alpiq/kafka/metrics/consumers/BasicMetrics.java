package com.alpiq.kafka.metrics.consumers;

import com.alpiq.kafka.metrics.dto.KafkaConfiguration;
import com.alpiq.kafka.metrics.service.BasicMetricsService;
import com.alpiq.kafka.metrics.service.KafkaConfigurationService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static java.lang.System.exit;

/**
 * BasicMetrics allows to consume the log frames from a Kafka topic defined in config.properties file.
 * It produces records in a Kafka topic defined in config.properties, each record contains the count
 * of unique users per minute based on the ts and uid fields extracted from the input Kafka topic.
 */
public class BasicMetrics {
    private final static Logger logger = LoggerFactory.getLogger(BasicMetrics.class.getName());

    public static void main(String[] args) {
        try {
            // Read the Kafka configuration for the consumer
            KafkaConfigurationService kafkaConfigurationService = new KafkaConfigurationService();
            KafkaConfiguration kafkaConfiguration = kafkaConfigurationService.getKafkaConfiguration();

            String bootstrapServers = kafkaConfiguration.getBootstrapServers();
            String consumerGroupId = kafkaConfiguration.getConsumerGroupId();
            String consumerTopicName = kafkaConfiguration.getConsumerTopicName();
            String producerTopicName = kafkaConfiguration.getProducerTopicName();

            // Create the consumer configuration
            Properties consumerProperties = new Properties();
            consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
            consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            // Create the producer configuration
            Properties producerProperties = new Properties();
            producerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            // Create the consumer
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProperties);
            // Subscribe the consumer to our topic
            consumer.subscribe(Collections.singleton(consumerTopicName));

            // Create the producer
            KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(producerProperties);

            // BasicMetricsService allows to process log frames and to produce the metrics
            BasicMetricsService basicMetricsService = new BasicMetricsService();
            basicMetricsService.setLogger(logger);
            basicMetricsService.setKafkaProducer(kafkaProducer);
            basicMetricsService.setKafkaProducerTopicName(producerTopicName);

            // Poll for new events
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    logger.debug("record " + record.value());

                    basicMetricsService.processRecord(record.value());
                }
                // Please note that the last count of records is not displayed because the log is expected to be infinite
                // but it is not. We could add some code here to solve this, but as we expect the stream to be infinite
                // this should never happen.
            }
        } catch (Exception e) {
            logger.error("BasicMetrics failed with " + e.getMessage());
            e.printStackTrace();
            exit(1);
        }
    }
}
