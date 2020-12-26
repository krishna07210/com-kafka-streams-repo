package com.kafka.streams.transformations.stateful;

import com.kafka.streams.common.CommonServices;
import com.kafka.streams.transformations.stateless.GroupByTransformation;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ReduceTransformation {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(GroupByTransformation.class);
        final Properties streamConfig = CommonServices.getStreamConfiguration("AggregatingTransformation");
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("favourite-colors-input", Consumed.with(Serdes.String(), Serdes.String()));
        stream.selectKey((key, value) -> value.split(",")[0])
                .mapValues((user, color) -> color.split(",")[1])
                .to("favourite-colors-intermediate", Produced.with(Serdes.String(), Serdes.String()));
        KTable<String, String> kTable = builder.table("favourite-colors-intermediate", Consumed.with(Serdes.String(), Serdes.String()));
        KGroupedTable<String, Integer> groupedTable =
                kTable.groupBy((key, value) -> KeyValue.pair(value, value.length()),
                        Grouped.with(Serdes.String(), Serdes.Integer()));
        groupedTable.reduce(
                (aggValue, newValue) -> aggValue + newValue,
                (aggValue, oldValue) -> aggValue - oldValue,
                Materialized.as("reduced-table-store"));
        Topology topology = builder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamConfig);
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}