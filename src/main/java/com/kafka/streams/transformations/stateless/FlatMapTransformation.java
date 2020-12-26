package com.kafka.streams.transformations.stateless;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static com.kafka.streams.common.CommonServices.getStreamConfiguration;

public class FlatMapTransformation {
    public static void main(String[] args) {
        final Properties streamConfiguration = getStreamConfiguration("FlatMap-Transformation");
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("product-details", Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, Integer> newStream = stream.flatMap((key, value) -> {
            List<KeyValue<String, Integer>> result = new LinkedList<>();
            result.add(KeyValue.pair(value.toUpperCase(),1000));
            result.add(KeyValue.pair(value.toUpperCase(),20000));
            return result;
        });
    }
}
