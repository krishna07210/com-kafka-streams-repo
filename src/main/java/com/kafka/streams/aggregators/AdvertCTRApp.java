package com.kafka.streams.aggregators;

import com.kafka.model.AdClick;
import com.kafka.model.AdClickCTR;
import com.kafka.model.AdImpression;
import com.kafka.model.CampaignPerformance;
import com.kafka.serde.AppSerdes;
import com.kafka.streams.common.StreamsConstants;
import com.kafka.streams.topologies.CTRTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class AdvertCTRApp {
    private static final Logger logger = LoggerFactory.getLogger(AdvertCTRApp.class);

    public static void main(String[] args) {
        Properties props = new Properties();
         final  String impressionTopic = "ad-impressions";
         final  String clicksTopic = "ad-clicks";
         final  String stateStoreName = "tmp/state-store";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,"AdvertCTRApp");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, StreamsConstants.BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreName);
        //props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        CTRTopology.withBuilder(streamsBuilder);

        logger.info("Starting Stream...");
        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Streams...");
            streams.close();
        }));

    }
}
