package com.kafka.streams.joins.KStreamToKStream;

import com.kafka.model.PaymentConfirmation;
import com.kafka.model.PaymentRequest;
import com.kafka.model.SimpleInvoice;
import com.kafka.model.TransactionStatus;
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
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Properties;

public class PaymentProcessApp {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(TimestampWindowExtractApp.class);
        final String paymentRequestTopicName = "payment_request";
        final String paymentConfirmationTopicName = "payment_confirmation";
        final Properties streamConfig = CommonServices.getStreamConfigurationNoSerdes("TimestampInvoicesExtractApp");
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, PaymentRequest> KS0 = streamsBuilder.stream(paymentRequestTopicName,
                Consumed.with(AppSerdes.String(), AppSerdes.PaymentRequestRecord())
                        .withTimestampExtractor(AppTimestampExtractor.PaymentRequest()));

        KStream<String, PaymentConfirmation> KS1 = streamsBuilder.stream(paymentConfirmationTopicName,
                Consumed.with(AppSerdes.String(), AppSerdes.PaymentConfirmationRecord())
                        .withTimestampExtractor(AppTimestampExtractor.PaymentConfirmation()));

        KS0.join(KS1, (KS0Val, KS1Val) ->
                        new TransactionStatus()
                                .withTransactionID(KS0Val.getTransactionID())
                                .withStatus((KS0Val.getOTP().equals(KS1Val.getOTP()) ? "Success" : "Failure")),
                JoinWindows.of(Duration.ofMinutes(5)),
                Joined.with(AppSerdes.String(),
                        AppSerdes.PaymentRequestRecord(),
                        AppSerdes.PaymentConfirmationRecord())
        ).print(Printed.toSysOut());

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), streamConfig);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Streams");
            streams.close();
        }));
    }
}
