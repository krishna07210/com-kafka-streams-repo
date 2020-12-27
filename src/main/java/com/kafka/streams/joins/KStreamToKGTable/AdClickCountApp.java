package com.kafka.streams.joins.KStreamToKGTable;

import com.kafka.model.*;
import com.kafka.serde.AppSerdes;
import com.kafka.streams.TimestampExtractors.AppTimestampExtractor;
import com.kafka.streams.common.CommonServices;
import com.kafka.streams.windowStreamsApps.TimestampWindowExtractApp;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public class AdClickCountApp {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(TimestampWindowExtractApp.class);
        final String inventoryTopic = "active-inventories";
        final String clicksTopic = "ad-clicks";
        final Properties streamConfig = CommonServices.getStreamConfigurationNoSerdes("TimestampInvoicesExtractApp");
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        GlobalKTable<String, AdInventories> GKT0 = streamsBuilder.globalTable(inventoryTopic,
                Consumed.with(AppSerdes.String(), AppSerdes.AdInventoryRecord())
        );

        KStream<String, AdClick> KS1 = streamsBuilder.stream(clicksTopic,
                Consumed.with(AppSerdes.String(), AppSerdes.AdClickRecord())
        );

        KS1.join(GKT0, (k, v) -> k, (v1, v2) -> v2)
                .groupBy((k, v) -> v.getNewsType(), Grouped.with(AppSerdes.String(), AppSerdes.AdInventoryRecord()))
                .count()
                .toStream().print(Printed.toSysOut());
        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), streamConfig);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Streams");
            streams.close();
        }));
    }
}
