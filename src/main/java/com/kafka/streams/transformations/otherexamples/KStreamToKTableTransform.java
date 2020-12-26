package com.kafka.streams.transformations.otherexamples;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.kafka.streams.common.CommonServices.getStreamConfiguration;

import java.util.Properties;

public class KStreamToKTableTransform {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(KStreamToKTableTransform.class);
        final Properties streamConfiguration = getStreamConfiguration("KStreamToKTableTransform");
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("products", Consumed.with(Serdes.String(), Serdes.String()));
        stream.foreach((x, y) -> System.out.println(x + "=>" + y));
        //First way
        KTable<String,Long> kTable = stream.groupByKey().count();
        kTable.toStream().foreach((x,y) -> System.out.println(x + "=>" +y));
        //Second way : Produce to intermediate Topic and then consume
        stream.to("intermediate-product-topic", Produced.with(Serdes.String(), Serdes.String()));
        KTable<String ,String> table = builder.table("intermediate-product-topic",Consumed.with(Serdes.String(),Serdes.String()));
        logger.info(":: Print KTable Output");
//        table.toStream().foreach((x,y) -> System.out.println(x + "=> " +y));
        table.toStream().to("product-output",Produced.with(Serdes.String(),Serdes.String()));
        Topology topology = builder.build();
        logger.info("Topology Describe :: {} ",topology.describe());
        KafkaStreams kafkaStreams = new KafkaStreams(topology,streamConfiguration);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }
}
