package com.alpiq.kafka.metrics.dto;

public class KafkaConfiguration {
    private String bootstrapServers;
    private String consumerGroupId, consumerTopicName;
    private String producerTopicName;

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getConsumerGroupId() {
        return consumerGroupId;
    }

    public void setConsumerGroupId(String consumerGroupId) {
        this.consumerGroupId = consumerGroupId;
    }

    public String getConsumerTopicName() {
        return consumerTopicName;
    }

    public void setConsumerTopicName(String consumerTopicName) {
        this.consumerTopicName = consumerTopicName;
    }

    public void setProducerTopicName(String producerTopicName) {
        this.producerTopicName = producerTopicName;
    }

    public String getProducerTopicName() {
        return producerTopicName;
    }
}
