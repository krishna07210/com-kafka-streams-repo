package com.kafka.producers.producerApps;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BankTransactionsProducer {
    public static void main(String[] args) {
        String topicName = "bank-transactions";
        final Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //Producer acks
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "3");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        //leverage idempotent producer from kafka 0.11, true ensures don't push duplicates
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        int i = 0;
        while (true) {
            System.out.println("Producting Batch:: " + i);
            try {
                producer.send(newRandomTransaction(topicName, "HARI"));
                Thread.sleep(100);
                producer.send(newRandomTransaction(topicName, "LIKITH"));
                Thread.sleep(100);
                producer.send(newRandomTransaction(topicName, "JESI"));
                Thread.sleep(100);
                i += 1;
            } catch (InterruptedException e) {
                break;
            }
        }

    }

    public static ProducerRecord<String, String> newRandomTransaction(String topic, String clientName) {
        ObjectNode transaction = JsonNodeFactory.instance.objectNode();
        Integer amount = ThreadLocalRandom.current().nextInt(0, 100);
        Instant now = Instant.now();
        transaction.put("name", clientName);
        transaction.put("amount", amount);
        transaction.put("time", now.toString());
        return new ProducerRecord<>(topic, clientName, transaction.toString());
    }
}
