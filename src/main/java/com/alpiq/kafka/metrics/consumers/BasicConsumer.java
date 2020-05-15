package com.alpiq.kafka.metrics.consumers;

import com.alpiq.kafka.metrics.config.ConsumerConfiguration;
import com.alpiq.kafka.metrics.dto.ConsumerConfigurationDTO;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;

import static java.lang.System.exit;

/**
 * BasicConsumer allows to consume the records from a Kafka topic defined in the config.properties file
 * found in the resources directory of this project.
 */
public class BasicConsumer {
    private static boolean firstTime = true;
    private static String previousTs = "";
    private static HashSet<String> uniqueUsers = new HashSet<String>();
    private final static Logger logger = LoggerFactory.getLogger(BasicConsumer.class.getName());

    public static void main(String[] args) throws IOException {
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
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    logger.debug("record " + record.value());

                    processRecord(record.value());
                }
                // Please note that the last count of records is not displayed because the log is expected to be infinite
                // but it is not. We could add some code here to solve this, but as we expect the stream to be infinite
                // this should never happen.
            }
        } catch (Exception e) {
            logger.error("BasicConsumer failed with " + e.getMessage());
            e.printStackTrace();
            exit(1);
        }
    }

    /**
     * Allows to convert a unix timestamp given as a string (ex: 1468244384) to a human readable timestamp
     * whose granularity is the minute (ex: 2016-07-11 02:39).
     *
     * @param ts A unix timestamp
     * @return The same unix timestamp converted to a human readable format, granularity is the minute
     * @throws IllegalArgumentException An exception may occur when converting the timestamp to a long
     */
    private static String toMinute(String ts) throws IllegalArgumentException {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Timestamp(Long.parseLong(ts) * 1000));
    }

    /**
     * Processing a log record coming from Kafka, the record is a string representation of a json structure.
     * The json structure must contain a ts and uid node.
     * From the ts node which is a unix timestamp value, we produce a timestamp as a string whose granularity
     *   is the minute (ex: 2020/05/15 14:50)
     * We store the uid in a collection containing unique uid values.
     * We detect a change of this "minute" timestamp from one record to the other, and we report the count
     *   of elements in the uid collection (which is a set so unique values only) when there's a change
     *   of minute from one record to the other.
     *
     * @param record A string containing a json representation of the log frame, with a ts and uid node.
     */
    private static void processRecord(String record) {
        // We need a json parser to extract the ts and uid fields from the json structure
        JSONParser parser = new JSONParser();

        try {
            JSONObject jsonObject = (JSONObject) parser.parse(record);

            String ts = jsonObject.get("ts").toString();
            String uid = jsonObject.get("uid").toString();
            String currentTs = toMinute(ts);

            logger.debug("Processing currentTs " + currentTs + " ts " + ts + " uid " + uid);

            if (firstTime) {
                previousTs = currentTs;
                firstTime = false;
            }

            if (currentTs.equals(previousTs)) {
                uniqueUsers.add(uid);
            } else {
                logger.info("At " + previousTs + " we have a unique users count of " + uniqueUsers.size());
                previousTs = currentTs;

                // Empty the list of uid
                uniqueUsers.clear();
                uniqueUsers.add(uid);
            }
        } catch (Exception e) {
            logger.error("Timestamp conversion error, the current record is rejected: " + e.getMessage());
        }
    }
}
