package com.kafka.streams.aggregators;

import com.kafka.serde.AppSerdes;
import com.kafka.streams.common.CommonServices;
import com.kafka.streams.transformations.stateless.GroupByTransformation;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class AgeWiseCountApp {
    public static void main(String[] args) {
        final String topicName = "person-ages";
        final Logger logger = LoggerFactory.getLogger(GroupByTransformation.class);
        final Properties streamConfig = CommonServices.getStreamConfigurationNoSerdes("RewardsApp");
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.table(topicName, Consumed.with(AppSerdes.String(), AppSerdes.String()))
                .groupBy((person, age) -> KeyValue.pair(age, person), Grouped.with(Serdes.String(), Serdes.String()))
                .count()
                .toStream().foreach((k, v) -> System.out.println(k + "=>" + v));
        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), streamConfig);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Streams");
            streams.close();
        }));
    }
}
