package com.kafka.streams.streamsApps;

import com.kafka.model.PosInvoice;
import com.kafka.producers.serde.AppSerdes;
import com.kafka.streams.common.PosFanoutAppConstants;
import com.kafka.streams.common.StreamsConstants;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class PosFanoutApp {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(PosFanoutApp.class);
        final Properties props = new Properties();
        String applicationId = "PosFanout";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, StreamsConstants.bootstrapServers);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, PosInvoice> KS0 = builder.stream(PosFanoutAppConstants.posTopicName,
                Consumed.with(AppSerdes.String(), AppSerdes.PosInvoice()));
        KS0.filter((k, v) ->
                v.getDeliveryType().equals(PosFanoutAppConstants.DELIVERY_TYPE_HOME_DELIVERY))
                .to(PosFanoutAppConstants.shipmentTopicName, Produced.with(AppSerdes.String(), AppSerdes.PosInvoice()));

        KS0.filter((k, v) -> v.getCustomerType().equalsIgnoreCase(PosFanoutAppConstants.CUSTOMER_TYPE_PRIME))
                .mapValues(invoice -> RecordBuilder.getNotification(invoice))
                .to(PosFanoutAppConstants.notificationTopic, Produced.with(AppSerdes.String(), AppSerdes.Notification()));
        KS0.mapValues(invoice -> RecordBuilder.getMaskedInvoice(invoice))
                .flatMapValues(invoice -> RecordBuilder.getHadoopRecords(invoice))
                .to(PosFanoutAppConstants.hadoopTopic, Produced.with(AppSerdes.String(), AppSerdes.HadoopRecord()));
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Stream");
            streams.close();
        }));
    }
}
