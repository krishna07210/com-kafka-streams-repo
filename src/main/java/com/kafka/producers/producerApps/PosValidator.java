package com.kafka.producers.producerApps;

import com.kafka.producers.common.ProducerConstants;
import com.kafka.serde.JsonDeserializer;
import com.kafka.serde.JsonSerializer;
import com.kafka.model.PosInvoice;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class PosValidator {
    private static final Logger logger = LoggerFactory.getLogger(PosValidator.class);

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(String[] args) {
        final String validTopicName = "valid-pos";
        final String invalidTopicName = "invalid-pos";
        final String groupId = "PosValidatorGroup";
        final String applicationId = "PosValidator";
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, applicationId);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ProducerConstants.BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, PosInvoice.class);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, PosInvoice> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(ProducerConstants.SOURCE_TOPIC_NAMES));

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, applicationId);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ProducerConstants.BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        KafkaProducer<String, PosInvoice> producer = new KafkaProducer<>(producerProps);

        while (true) {
            ConsumerRecords<String, PosInvoice> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, PosInvoice> record : records) {
                if (record.value().getDeliveryType().equals("HOME-DELIVERY") &&
                        record.value().getDeliveryAddress().getContactNumber().equals("")) {
                    //Invalid
                    producer.send(new ProducerRecord<>(invalidTopicName, record.value().getStoreID(),
                            record.value()));
                    logger.info("invalid record - " + record.value().getInvoiceNumber());
                } else {
                    //Valid
                    producer.send(new ProducerRecord<>(validTopicName, record.value().getStoreID(),
                            record.value()));
                    logger.info("valid record - " + record.value().getInvoiceNumber());
                }
            }
        }
    }
}
