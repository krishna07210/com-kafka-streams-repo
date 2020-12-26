package com.kafka.streams.requirements;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

import static com.kafka.streams.common.CommonServices.getStreamConfiguration;

/**
 * Requirement 1 :  Find Favourite Color from comma separated topic
 * Input Topic :    favourite-colors-input
 * Output Topic:    favourite-colors-output
 */

public class FindFavouriteColor {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(FindFavouriteColor.class);
        final Properties streamConfig = getStreamConfiguration("FindFavouriteColor");
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("favourite-colors-input", Consumed.with(Serdes.String(), Serdes.String()));
//        KTable<String, Long> kTable = stream.selectKey((key, value) -> value.split(",")[1])
//                .map((x, y) -> KeyValue.pair(x, y.split(",")[0]))
//                .filter((x, y) -> !x.contains("black"))
//                .groupByKey()
//                .count() ;
        stream.filter((key, value) -> value.contains(","))
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                .mapValues(value -> value.split(",")[1].toLowerCase())
                .filter((user, color) -> Arrays.asList("green", "red", "blue").contains(color))
                .to("favourite-colors-intermediate", Produced.with(Serdes.String(), Serdes.String()));

        KTable<String, String> userKeyValue = builder.table("favourite-colors-intermediate", Consumed.with(Serdes.String(), Serdes.String()));
        KTable<String, Long> favouriteColors = userKeyValue
                .groupBy((user, color) -> new KeyValue<>(color, color))
                .count();
        favouriteColors.toStream().to("favourite-colors-output", Produced.with(Serdes.String(), Serdes.Long()));

        favouriteColors.toStream().foreach((x, y) -> System.out.println(x + " => " + y));
        Topology topology = builder.build();
        logger.info("Topology Describe :: {} ", topology.describe());
        KafkaStreams streams = new KafkaStreams(topology, streamConfig);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
