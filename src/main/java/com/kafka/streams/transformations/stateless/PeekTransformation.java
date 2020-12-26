package com.kafka.streams.transformations.stateless;

import com.kafka.streams.common.CommonServices;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class PeekTransformation {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(GroupByTransformation.class);
        final Properties streamConfig = CommonServices.getStreamConfiguration("PeekTransformation");
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("favourite-colors-input", Consumed.with(Serdes.String(), Serdes.String()));
        stream.peek(
                (key, value) -> System.out.println("key=" + key + ", value=" + value)
        );
        Topology topology = builder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamConfig);
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
