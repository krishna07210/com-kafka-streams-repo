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

public class TransactionProducer {
    private static final Logger logger = LoggerFactory.getLogger(TransactionProducer.class);

    public static void main(String[] args) {

        final String topicName1 = "trans-producer-1";
        final String topicName2 = "trans-producer-2";
        final String applicationId = "transaction-producer";
        final int numEvents = 2;
        logger.info("Creating Kafka Producer...");
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, applicationId);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ProducerConstants.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, ProducerConstants.transaction_id);

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
        producer.initTransactions();

        logger.info("Starting First Transaction...");
        producer.beginTransaction();
        try {
            for (int i = 1; i <= numEvents; i++) {
                producer.send(new ProducerRecord<>(topicName1, i, "Simple Message-T1-" + i));
                producer.send(new ProducerRecord<>(topicName2, i, "Simple Message-T1-" + i));
            }
            logger.info("Committing First Transaction.");
            producer.commitTransaction();
        } catch (Exception e) {
            logger.error("Exception in First Transaction. Aborting...");
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException(e);
        }

        logger.info("Starting Second Transaction...");
        producer.beginTransaction();
        try {
            for (int i = 1; i <= numEvents; i++) {
                producer.send(new ProducerRecord<>(topicName1, i, "Simple Message-T2-" + i));
                producer.send(new ProducerRecord<>(topicName2, i, "Simple Message-T2-" + i));
            }
            logger.info("Aborting Second Transaction.");
            producer.abortTransaction();
        } catch (Exception e) {
            logger.error("Exception in Second Transaction. Aborting...");
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException(e);
        }
        logger.info("Finished - Closing Kafka Producer.");
        producer.close();
    }
}
