package com.kafka.streams.topologies;

import com.kafka.model.AdClickCTR;
import com.kafka.model.AdImpression;
import com.kafka.model.CampaignPerformance;
import com.kafka.serde.AppSerdes;
import com.kafka.streams.aggregators.AdvertCTRApp;
import com.kafka.streams.common.StreamsConstants;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class CTRTopology {
    private static final Logger logger = LoggerFactory.getLogger(AdvertCTRApp.class);

    public static void withBuilder(StreamsBuilder streamsBuilder) {
        final String impressionTopic = "ad-impressions";
        final String clicksTopic = "ad-clicks";
        final String stateStoreName = "tmp/state-store";
        final String stateStoreLocationUT = "tmp/ut/state-store";
        final String stateStoreNameCP = "cp-store";
        final String outputTopic = "campaign-performance";

        KStream<String, AdImpression> KS0 = streamsBuilder.stream(impressionTopic,
                Consumed.with(AppSerdes.String(), AppSerdes.AdImpression()));

        KTable<String, Long> adImpressionCount = KS0.groupBy((k, v) -> v.getCampaigner(),
                Grouped.with(AppSerdes.String(), AppSerdes.AdImpression()))
                .count();

        KStream<String, AdClickCTR> KS1 = streamsBuilder.stream(clicksTopic,
                Consumed.with(AppSerdes.String(), AppSerdes.AdClickCTR()));

        KTable<String, Long> adClickCount = KS1.groupBy((k, v) -> v.getCampaigner(),
                Grouped.with(AppSerdes.String(), AppSerdes.AdClickCTR()))
                .count();

        KTable<String, CampaignPerformance> campaignPerformance = adImpressionCount.leftJoin(
                adClickCount, (impCount, clkCount) -> new CampaignPerformance()
                        .withAdImpressions(impCount)
                        .withAdClicks(clkCount))
                .mapValues((k, v) -> v.withCampaigner(k),
                        Materialized.<String, CampaignPerformance, KeyValueStore<Bytes, byte[]>>
                                as(stateStoreNameCP)
                                .withKeySerde(AppSerdes.String())
                                .withValueSerde(AppSerdes.CampaignPerformance()));

        campaignPerformance.toStream().to(outputTopic,
                Produced.with(AppSerdes.String(), AppSerdes.CampaignPerformance()));

        campaignPerformance.toStream().foreach((k, v) -> logger.info("{}", v));
    }
}
