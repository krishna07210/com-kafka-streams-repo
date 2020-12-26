package com.kafka.streams.streamsApps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

import static com.kafka.streams.common.CommonServices.getStreamConfiguration;

public class WordCount {
    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream("word-count-input");
        KTable<String, Long> wordCounts =
                textLines.mapValues(textLine -> textLine.toLowerCase())
                        .flatMapValues(textLine -> Arrays.asList(textLine.split("\\W+")))
                        .selectKey((key, word) -> word)
                        .groupByKey()
                        .count(Materialized.as("Counts"));
        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(WordCount.class);
        final Properties streamConfig = getStreamConfiguration("word-count-app");
        WordCount wordCount = new WordCount();
        KafkaStreams streams = new KafkaStreams(wordCount.createTopology(),streamConfig);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        while (true){
            streams.localThreadsMetadata().forEach(data->System.out.println(data));
            try{
                Thread.sleep(5000);
            }catch (InterruptedException e){
                break;
            }
        }
    }
}
