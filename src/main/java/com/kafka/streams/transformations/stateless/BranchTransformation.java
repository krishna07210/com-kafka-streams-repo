package com.kafka.streams.transformations.stateless;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

import static com.kafka.streams.common.CommonServices.getStreamConfiguration;

public class BranchTransformation {
    public static void main(String[] args) {
        final Properties streamConfiguration = getStreamConfiguration("branch-Transformation");
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("product-details");
        KStream<String, String>[] branches = stream.branch(
                (key, value) -> value.contains("Amazon"),
                (key, value) -> value.contains("Flipkart"),
                (key, value) -> true
        );
        KStream<String,String> amazonStream = branches[0];
        amazonStream.foreach((key, value) -> System.out.println(key + " => " + value));
        KStream<String,String> flipkartStream = branches[1];
        flipkartStream.foreach((key,value) -> System.out.println(key + " => " +value));
        KStream<String,String> othertStream = branches[2];
        othertStream.foreach((key,value) -> System.out.println(key + " => " +value));
        final Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology,streamConfiguration);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
