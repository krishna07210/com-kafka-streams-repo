package com.kafka.streams.infiniteStreamsApps;

import com.kafka.model.PosInvoice;
import com.kafka.serde.AppSerdes;
import com.kafka.streams.common.CommonServices;
import com.kafka.streams.common.PosFanoutAppConstants;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class PosFanoutApp {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(PosFanoutApp.class);
        final Properties props = CommonServices.getStreamConfigurationNoSerdes("Pos-Fanout");
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, PosInvoice> KS0 = builder.stream(PosFanoutAppConstants.POS_TOPIC_NAME,
                Consumed.with(AppSerdes.String(), AppSerdes.PosInvoice()));
        KS0.filter((k, v) ->
                v.getDeliveryType().equals(PosFanoutAppConstants.DELIVERY_TYPE_HOME_DELIVERY))
                .to(PosFanoutAppConstants.SHIPMENT_TOPIC_NAME, Produced.with(AppSerdes.String(), AppSerdes.PosInvoice()));

        KS0.filter((k, v) -> v.getCustomerType().equalsIgnoreCase(PosFanoutAppConstants.CUSTOMER_TYPE_PRIME))
                .mapValues(invoice -> RecordBuilder.getNotification(invoice))
                .to(PosFanoutAppConstants.NOTIFICATION_TOPIC, Produced.with(AppSerdes.String(), AppSerdes.Notification()));
        KS0.mapValues(invoice -> RecordBuilder.getMaskedInvoice(invoice))
                .flatMapValues(invoice -> RecordBuilder.getHadoopRecords(invoice))
                .to(PosFanoutAppConstants.HADOOP_TOPIC, Produced.with(AppSerdes.String(), AppSerdes.HadoopRecord()));
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Stream");
            streams.close();
        }));
    }
}
