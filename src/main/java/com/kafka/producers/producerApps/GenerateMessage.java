package com.kafka.producers.producerApps;

import com.kafka.producers.common.ProducerConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class GenerateMessage {
    private static final Logger logger = LoggerFactory.getLogger(GenerateMessage.class);

    public static void main(String[] args) {
        logger.info("Creating Kafka Producer...");
        final int numEvents = 100000;
        final String applicationId ="generate-message";
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, applicationId);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ProducerConstants.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

        logger.info("Start sending messages...");
        for (int i = 1; i <= numEvents; i++) {
            producer.send(new ProducerRecord<>(ProducerConstants.topicName, i, "Simple Message-" + i));
        }
        logger.info("Finished - Closing Kafka Producer.");
        producer.close();

    }
}

