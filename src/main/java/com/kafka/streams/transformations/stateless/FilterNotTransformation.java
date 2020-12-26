package com.kafka.streams.transformations.stateless;

import com.kafka.streams.common.CommonServices;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class FilterNotTransformation {
    public static void main(String[] args) {
        final Properties streamConfiguration = CommonServices.getStreamConfiguration("FilterNot-Transformation");
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("product-details", Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> filteredStream = stream.filterNot((key, value) -> key.contains("Reliance"));
        filteredStream.foreach((k, v) -> System.out.println(k + " => " + v));
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, streamConfiguration);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
