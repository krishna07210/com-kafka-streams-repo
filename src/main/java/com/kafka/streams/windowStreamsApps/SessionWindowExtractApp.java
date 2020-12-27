package com.kafka.streams.windowStreamsApps;

import com.kafka.model.SimpleInvoice;
import com.kafka.model.UserClicks;
import com.kafka.producers.serde.AppSerdes;
import com.kafka.streams.TimestampExtractors.InvoiceTimeExtractor;
import com.kafka.streams.common.CommonServices;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Properties;

/**
 * Count number of user clicks for user Session
 */
public class SessionWindowExtractApp {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(SessionWindowExtractApp.class);
        final String topicName = "user-clicks";
        final Properties streamConfig = CommonServices.getStreamConfigurationNoSerdes("SessionWindowExtractApp");
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, UserClicks> KS0 = streamsBuilder.stream(topicName,
                Consumed.with(AppSerdes.String(), AppSerdes.UserClicksRecord())
                        .withTimestampExtractor(new InvoiceTimeExtractor()));
        //First Level Group at StoreId level, Second at Window Level
        KTable<Windowed<String>, Long> KT0 =
                KS0.groupByKey(Grouped.with(AppSerdes.String(), AppSerdes.UserClicksRecord()))
                        .windowedBy(SessionWindows.with(Duration.ofMinutes(5)))
                        .count();

        KT0.toStream().foreach(
                (wKey, value) -> logger.info(
                        "Store ID: {}  ; Window ID: {} ; Window Start: {} ; Window End: {}  = Count: {} ",
                        wKey.key(),
                        wKey.window().hashCode(),
                        Instant.ofEpochMilli(wKey.window().start()).atOffset(ZoneOffset.UTC),
                        Instant.ofEpochMilli(wKey.window().end()).atOffset(ZoneOffset.UTC),
                        value)
        );
        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), streamConfig);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Streams");
            streams.close();
        }));
    }
}
