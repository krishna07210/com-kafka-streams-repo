package com.kafka.streams.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class ConsumerOffsetReset {
    public static void main(String[] args) {
    }

    public static String resetConsumerGroupOffsets(String applicationName) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, applicationName);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return "S";
    }
}
