package com.alpiq.kafka.metrics.service;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.HashSet;

public class BasicMetricsService {
    public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm";

    private static boolean firstTime = true;
    private String previousTs = "";
    private HashSet<String> uniqueUsers = new HashSet<String>();
    private Logger logger;
    private KafkaProducer<String, String> kafkaProducer;
    private String kafkaProducerTopicName;

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    public void setKafkaProducer(KafkaProducer<String, String> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    public void setKafkaProducerTopicName(String kafkaProducerTopicName) {
        this.kafkaProducerTopicName = kafkaProducerTopicName;
    }

    /**
     * Allows to convert a unix timestamp given as a string (ex: 1468244384) to a human readable timestamp
     * whose granularity is the minute (ex: 2016-07-11 14:39).
     *
     * @param ts A unix timestamp
     * @return The same unix timestamp converted to a human readable format, granularity is the minute
     * @throws IllegalArgumentException An exception may occur when converting the timestamp to a long
     */
    private String toMinute(String ts) throws IllegalArgumentException {
        return new SimpleDateFormat(TIMESTAMP_FORMAT).format(new Timestamp(Long.parseLong(ts) * 1000));
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
    public void processRecord(String record) {
        // We need a json parser to extract the ts and uid fields from the json structure
        JSONParser parser = new JSONParser();

        try {
            JSONObject jsonObject = (JSONObject) parser.parse(record);

            String ts = jsonObject.get("ts").toString();
            String uid = jsonObject.get("uid").toString();
            String currentTs = this.toMinute(ts);

            logger.debug("Processing currentTs " + currentTs + " ts " + ts + " uid " + uid);

            if (firstTime) {
                previousTs = currentTs;
                firstTime = false;
            }

            if (currentTs.equals(previousTs)) {
                uniqueUsers.add(uid);
            } else {
                logger.info("At " + previousTs + " we have a unique users count of " + uniqueUsers.size());
                publishMetric(previousTs, uniqueUsers.size());
                previousTs = currentTs;

                // Empty the list of uid
                uniqueUsers.clear();
                uniqueUsers.add(uid);
            }
        } catch (Exception e) {
            logger.error("Exception caught, the current record is rejected: " + e.getMessage());
        }
    }

    private void publishMetric(String ts, int uniqueUsersCount) {
        JSONObject uniqueUsersMetrics = new JSONObject();
        uniqueUsersMetrics.put("tsMinute", ts);
        uniqueUsersMetrics.put("uniqueUsersCount", uniqueUsersCount);

        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<String, String>(kafkaProducerTopicName, uniqueUsersMetrics.toJSONString());

        logger.info("publishing to topic " + kafkaProducerTopicName + " : " + uniqueUsersMetrics.toJSONString());
        kafkaProducer.send(producerRecord, new BasicMetricsCallback());
    }

    private static class BasicMetricsCallback implements Callback {
        @Override public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null ) {
                e.printStackTrace();
            }
        }
    }
}
