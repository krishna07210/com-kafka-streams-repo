package com.kafka.streams.infiniteStreamsApps;

import com.kafka.model.RewardsNotification;
import com.kafka.producers.serde.AppSerdes;
import com.kafka.streams.common.CommonServices;
import com.kafka.streams.common.RewardsAppConstants;
import com.kafka.streams.transformations.stateless.GroupByTransformation;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import com.kafka.producers.datagenerator.RewardsNotifications;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * In-Memoery State Store Example
 */
public class RewardsAppWithReducer {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(GroupByTransformation.class);
        final Properties streamConfig = CommonServices.getStreamConfigurationNoSerdes("RewardsApp");
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, RewardsNotification> KS0 = builder.stream(RewardsAppConstants.POS_TOPIC_NAME,
                Consumed.with(AppSerdes.String(), AppSerdes.PosInvoice()))
                .filter((key, value) ->
                        value.getCustomerType().equalsIgnoreCase(RewardsAppConstants.CUSTOMER_TYPE_PRIME))
                .map((key, invoice) -> new KeyValue<>(
                        invoice.getCustomerCardNo(),
                        RewardsNotifications.getNotificationFrom(invoice)
                ));
        KGroupedStream<String, RewardsNotification> KGS0 = KS0.groupByKey(Grouped.with(AppSerdes.String(), AppSerdes.RewardsNotification()));
        KTable<String, RewardsNotification> KT01 = KGS0.reduce((aggValue, newValue) -> {
            newValue.setTotalLoyaltyPoints(newValue.getEarnedLoyaltyPoints() + aggValue.getTotalLoyaltyPoints());
            return newValue;
        });
        KT01.toStream().to(RewardsAppConstants.NOTIFICATION_TOPIC,
                Produced.with(AppSerdes.String(), AppSerdes.RewardsNotification()));
        KafkaStreams streams = new KafkaStreams(builder.build(), streamConfig);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Streams");
            streams.close();
        }));
    }
}
