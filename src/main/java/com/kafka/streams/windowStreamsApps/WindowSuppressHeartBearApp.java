package com.kafka.streams.windowStreamsApps;

import com.kafka.model.HeartBeat;
import com.kafka.producers.serde.AppSerdes;
import com.kafka.streams.TimestampExtractors.AppTimestampExtractor;
import com.kafka.streams.common.CommonServices;
import com.kafka.streams.transformations.stateless.GroupByTransformation;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Properties;

/**
 * Send the Alert if the Heartbeat is delayed 60 seconds
 */

public class WindowSuppressHeartBearApp {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(WindowSuppressHeartBearApp.class);
        final String topicName = "heart-beat-topic";
        final Properties streamConfig = CommonServices.getStreamConfigurationNoSerdes("WindowSuppressHeartBearApp");
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, HeartBeat> KS0 = streamsBuilder.stream(topicName,
                Consumed.with(AppSerdes.String(), AppSerdes.HeartBeatRecord())
                        .withTimestampExtractor(new AppTimestampExtractor()));

        KTable<Windowed<String>, Long> KT01 =
                KS0.groupByKey(Grouped.with(AppSerdes.String(), AppSerdes.HeartBeatRecord()))
                        .windowedBy(TimeWindows.of(Duration.ofSeconds(60)).grace(Duration.ofSeconds(10)))
                        .count()
                        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));
        KT01.toStream().foreach(
                (wKey, value) -> logger.info(
                        "App ID: " + wKey.key() + " Window ID: " + wKey.window().hashCode() +
                                " Window start: " + Instant.ofEpochMilli(wKey.window().start()).atOffset(ZoneOffset.UTC) +
                                " Window end: " + Instant.ofEpochMilli(wKey.window().end()).atOffset(ZoneOffset.UTC) +
                                " Count: " + value +
                                (value > 2 ? " Application is Alive" : " Application Failed - Sending Alert Email...")
                )
        );

        logger.info("Starting Stream...");
    }
}
