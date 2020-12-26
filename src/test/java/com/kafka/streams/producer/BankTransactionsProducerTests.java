package com.kafka.streams.producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.producers.producerApps.BankTransactionsProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BankTransactionsProducerTests {

    @Test
    public void newRandomTransactionsTest() {
        String topicName = "bank-transactions";
        ProducerRecord<String, String> record = BankTransactionsProducer.newRandomTransaction(topicName, "HARI");
        String key = record.key();
        String value = record.value();
        assertEquals(key, "HARI");
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(value);
            assertEquals(node.get("name").asText(), "HARI");
            assertTrue("Amount should be less than 1oo", node.get("amount").asInt() < 100);
            System.out.println(value);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
