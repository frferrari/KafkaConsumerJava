package com.alpiq.kafka.metrics.config;

import com.alpiq.kafka.metrics.dto.ConsumerConfigurationDTO;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

public class ConsumerConfiguration {
    ConsumerConfigurationDTO consumerConfigurationDTO = new ConsumerConfigurationDTO();
    InputStream inputStream;

    public ConsumerConfigurationDTO getConsumerConfiguration() throws IOException {
        try {
            Properties prop = new Properties();
            String propFileName = "config.properties";

            inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }

            String bootstrapServers = prop.getProperty("bootstrapServers");
            String consumerGroupId = prop.getProperty("consumerGroupId");
            String consumerTopicName = prop.getProperty("consumerTopicName");

            consumerConfigurationDTO.setBootstrapServers(bootstrapServers);
            consumerConfigurationDTO.setGroupId(consumerGroupId);
            consumerConfigurationDTO.setTopicName(consumerTopicName);
        } catch (Exception e) {
            System.out.println("ConsumerConfiguration exception: " + e);
        } finally {
            inputStream.close();
        }
        return consumerConfigurationDTO;
    }
}
