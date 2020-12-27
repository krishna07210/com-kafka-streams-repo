package com.kafka.streams.infiniteStreamsApps;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class WordCountAppTest {

    TopologyTestDriver testDriver;
    StringSerializer stringSerializer = new StringSerializer();
    LongSerializer longSerializer = new LongSerializer();
    ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);

    @Before
    public void setUpTopologyTestDriver() {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        WordCountApp wordCount = new WordCountApp();
        testDriver = new TopologyTestDriver(wordCount.createTopology(), streamsConfiguration);
    }

    @After
    public void closeTestDriver() {
        testDriver.close();
    }

    public void pushNewInputRecord(String value) {
        testDriver.pipeInput(recordFactory.create("word-count-input", null, value));
    }

    public ProducerRecord<String, Long> readOutput() {
        return testDriver.readOutput("word-count-output", new StringDeserializer(), new LongDeserializer());
    }

    @Test
    public void makeSureCountsAreCorrect() {
        String firstExample = "testing Kafka Streams";
        pushNewInputRecord(firstExample);
        OutputVerifier.compareKeyValue(readOutput(), "testing", 1L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 1L);
        OutputVerifier.compareKeyValue(readOutput(), "streams", 1L);
        assertEquals(readOutput(), null);

        String secondExample = "testing kafka again";
        pushNewInputRecord(secondExample);
        OutputVerifier.compareKeyValue(readOutput(), "testing", 2L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 2L);
        OutputVerifier.compareKeyValue(readOutput(), "again", 1L);
    }

    @Test
    public void makeSureWordsBecomeLowerCase() {
        String upperCaseString = "KAFKA Kafka kafka";
        pushNewInputRecord(upperCaseString);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 1L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 2L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 3L);
    }
}
