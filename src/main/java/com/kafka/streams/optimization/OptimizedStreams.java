package com.kafka.streams.optimization;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;
public class OptimizedStreams {
    public static void main(String[] args) {
        final Properties properties = new Properties();

        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test-application");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092   ");
        properties.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> inputStream = builder.stream("inputTopic");

        final KStream<String, String> changedKeyStream = inputStream.selectKey((k, v) -> v.substring(0, 5));

        // first repartition
        changedKeyStream.groupByKey(Grouped.as("count-repartition"))
                .count(Materialized.as("count-store"))
                .toStream().to("count-topic", Produced.with(Serdes.String(), Serdes.Long()));

        // second repartition
        changedKeyStream.groupByKey(Grouped.as("windowed-repartition"))
                .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
                .count(Materialized.as("windowed-count-store"))
                .toStream().to("windowed-count",
                Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.Long()));

        final Topology topology = builder.build(properties);
        final KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        kafkaStreams.start();
    }
}
