package com.kafka.streams.infiniteStreamsApps;

import com.kafka.model.PosInvoice;
import com.kafka.serde.AppSerdes;
import com.kafka.streams.common.CommonServices;
import com.kafka.streams.common.RewardsAppConstants;
import com.kafka.streams.custompartitioners.RewardsCustomPartitioner;
import com.kafka.streams.transformations.stateless.GroupByTransformation;
import com.kafka.streams.transformations.transformers.RewardsTransformer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * In-Memoery State Store Example
 */
public class RewardsApp {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(GroupByTransformation.class);
        final Properties streamConfig = CommonServices.getStreamConfigurationNoSerdes("RewardsApp");
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, PosInvoice> KS0 = builder.stream(RewardsAppConstants.POS_TOPIC_NAME,
                Consumed.with(AppSerdes.String(), AppSerdes.PosInvoice()))
                .filter((key, value) ->
                        value.getCustomerType().equalsIgnoreCase(RewardsAppConstants.CUSTOMER_TYPE_PRIME));

        StoreBuilder keyValueStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(RewardsAppConstants.REWARDS_STORE_NAME),
                AppSerdes.String(), AppSerdes.Double());
        builder.addStateStore(keyValueStoreBuilder);
        // We have issue in this solution in case of multi threaded process
//        KS0.transformValues(() -> new RewardsTransformer(), RewardsAppConstants.REWARDS_STORE_NAME)
//                .to(RewardsAppConstants.NOTIFICATION_TOPIC,
//                        Produced.with(AppSerdes.String(), AppSerdes.RewardsNotification()));
        KS0.through(RewardsAppConstants.REWARDS_TEMP_TOPIC,
                Produced.with(AppSerdes.String(), AppSerdes.PosInvoice(), new RewardsCustomPartitioner()))
                .transformValues(() -> new RewardsTransformer(), RewardsAppConstants.REWARDS_STORE_NAME)
                .to(RewardsAppConstants.NOTIFICATION_TOPIC,
                        Produced.with(AppSerdes.String(), AppSerdes.RewardsNotification()));

        KafkaStreams streams = new KafkaStreams(builder.build(), streamConfig);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Streams");
            streams.close();
        }));
    }
}
