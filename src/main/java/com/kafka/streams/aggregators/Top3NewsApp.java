package com.kafka.streams.aggregators;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafka.model.*;
import com.kafka.serde.AppSerdes;
import com.kafka.streams.common.CommonServices;
import com.kafka.streams.common.StreamsConstants;
import com.kafka.streams.sorting.Top3NewsTypes;
import com.kafka.streams.windowStreamsApps.TimestampWindowExtractApp;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Top3NewsApp {
    private static final Logger logger = LoggerFactory.getLogger(Top3NewsApp.class);
    public static void main(String[] args) {
         final String inventoryTopic = "active-inventories";
        final String clicksTopic = "ad-clicks";
         final String stateStoreName = "tmp/state-store";
         final String top3AggregateKey = "top3NewsTypes";
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Top3NewsApp");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, StreamsConstants.BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreName);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        GlobalKTable<String, AdInventories> GKT0 = streamsBuilder.globalTable(inventoryTopic,
                Consumed.with(AppSerdes.String(), AppSerdes.AdInventoryRecord())
        );

        KStream<String, AdClick> KS1 = streamsBuilder.stream(clicksTopic,
                Consumed.with(AppSerdes.String(), AppSerdes.AdClickRecord())
        );

        KTable<String, Long> KT1 = KS1.join(GKT0, (k, v) -> k, (v1, v2) -> v2)
                .groupBy((k, v) -> v.getNewsType(), Grouped.with(AppSerdes.String(), AppSerdes.AdInventoryRecord()))
                .count();

        KT1.groupBy((k, v) -> {
                    ClicksByNewsType value = new ClicksByNewsType();
                    value.setNewsType(k);
                    value.setClicks(v);
                    return KeyValue.pair(top3AggregateKey, value);
                }, Grouped.with(AppSerdes.String(), AppSerdes.ClicksByNewsTypeRecord())
        ).aggregate(() -> new Top3NewsTypes(),
                (k, newV, aggV) -> {
                    aggV.add(newV);
                    return aggV;
                },
                (k, oldV, aggV) -> {
                    aggV.remove(oldV);
                    return aggV;
                },
                Materialized.<String, Top3NewsTypes, KeyValueStore<Bytes, byte[]>>
                        as("top3-clicks")
                        .withKeySerde(AppSerdes.String())
                        .withValueSerde(AppSerdes.Top3NewsTypes()))
                .toStream().foreach((k, v) -> {
            try {
                logger.info("k=" + k + " v= " + v.getTop3Sorted());
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        });

        logger.info("Starting Stream...");
        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Streams...");
            streams.close();
        }));

    }
}
