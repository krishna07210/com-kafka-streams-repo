package com.kafka.streams.streamsApps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.AbstractProcessor;

import java.util.Properties;

public class ProcessorAPIExample {
    public static void main(String[] args) {

        final Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test-application");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final Topology topology = new Topology();
        topology.addSource("SourceTopicProcessor", "input");
        topology.addProcessor("FilteringProcessor", FilterProcessor::new, "SourceTopicProcessor");
        topology.addProcessor("MappingProcessor", MapValuesProcessor::new, "FilteringProcessor");
        topology.addSink("SinkProcessor", "output", "MappingProcessor");

        System.out.println(topology.describe());
        final KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.start();
    }


    static class FilterProcessor extends AbstractProcessor<String, String> {
        @Override
        public void process(String key, String value) {
            if (value.endsWith("FOO")) {
                context().forward(key, value);
            }
        }
    }

    static class MapValuesProcessor extends AbstractProcessor<String, String> {
        @Override
        public void process(String key, String value) {
            context().forward(key, value.substring(0, 3));
            // Processor API gives flexibility to forward KV pairs
            // to arbitrary child nodes
            //context().forward(key, value.substring(0,3), To.child("some node"));
        }
    }

}
