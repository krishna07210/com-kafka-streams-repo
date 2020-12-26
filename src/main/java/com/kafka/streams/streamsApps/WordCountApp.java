package com.kafka.streams.streamsApps;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {
    public static void main(String[] args) {
        System.out.println("Hello Streams");
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        // 1. Get Stream from kafka
        final KStream<String, String> wordCountInput = builder.stream("word-count-input");
        // 2. map values to lowercase
        KTable<String, Long> wordCounts = wordCountInput.mapValues(textLine -> textLine.toLowerCase())
                // 3. flatmap values split by space
                .flatMapValues(lowerTextLine -> Arrays.asList(lowerTextLine.split("\\W+")))
                // 4. select key to apply a key
                .selectKey((key, word) -> word)
                // 5. group key before aggregation
                .groupByKey()
                //  6. count occurences
                .count();
        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));
        final Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, config);
        streams.start();
        //printing the topology
        System.out.println(streams.toString());
        //shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
 }

