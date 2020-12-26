package com.kafka.streams.transformations.otherexamples;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

import static com.kafka.streams.common.CommonServices.getStreamConfiguration;

public class KTableToKStreamTransform {
    public static void main(String[] args) {
        final Properties streamConfiguration = getStreamConfiguration("KTableToKStreamTransform");
        final StreamsBuilder builder = new StreamsBuilder();
        KTable<String, String> table = builder.table("product-details", Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String,String> stream = table.toStream();
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, streamConfiguration);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
