package com.kafka.streams.transformations.stateless;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.kafka.streams.common.CommonServices.getStreamConfiguration;

public class FilterTransformation {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(FilterTransformation.class);
        final Properties streamConfiguration = getStreamConfiguration("Filter-Transformation");
        final StreamsBuilder builder = new StreamsBuilder();
//        KStream<String,String> stream = builder.stream("product-details", Consumed.with(Serdes.String(),Serdes.String()));
//        KStream<String,String> filteredStream = stream.filter((key,value) -> key.contains("Reliance"));
//        filteredStream.foreach((k,v) -> System.out.println(k +" => "+ v));

        KTable<String, String> table = builder.table("product-details", Consumed.with(Serdes.String(), Serdes.String()));
        KTable<String, String> filteredTable = table.filter((key, value) -> value.contains("Amazon"));
        filteredTable.toStream().foreach((k, v) -> System.out.println(k + " => " + v));

        Topology topology = builder.build();
        logger.info("Topology Describe :: {} ",topology.describe());
        KafkaStreams streams = new KafkaStreams(topology, streamConfiguration);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
