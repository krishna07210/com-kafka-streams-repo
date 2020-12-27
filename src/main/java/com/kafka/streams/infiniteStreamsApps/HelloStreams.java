package com.kafka.streams.infiniteStreamsApps;

import com.kafka.streams.common.StreamsConstants;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class HelloStreams {
    private static final Logger logger = LoggerFactory.getLogger(HelloStreams.class);

    public static void main(String[] args) {
        String applicationID = "HelloStreams";
        String topicName = "hello-producer-topic";
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, StreamsConstants.BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<Integer, String> kStream = streamsBuilder.stream(topicName);
        kStream.foreach((k, v) -> System.out.println("Key= " + k + " Value= " + v));
        //kStream.peek((k,v)-> System.out.println("Key= " + k + " Value= " + v));

        Topology topology = streamsBuilder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        logger.info("Starting stream.");
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down stream");
            streams.close();
        }));
    }
}
