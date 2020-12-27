package com.kafka.streams.TimestampExtractors;

import com.kafka.model.HeartBeat;
import com.kafka.model.PaymentConfirmation;
import com.kafka.model.PaymentRequest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Instant;

public class AppTimestampExtractor {

    public static final class HeartBeatTimeExtractor implements TimestampExtractor {
        @Override
        public long extract(ConsumerRecord<Object, Object> consumerRecord, long prevTime) {
            HeartBeat heartBeat = (HeartBeat) consumerRecord.value();
            Long heartBeatTime = Instant.parse(heartBeat.getCreatedTime()).toEpochMilli();
            return (heartBeatTime > 0 ? heartBeatTime : prevTime);
        }
    }

    public static HeartBeatTimeExtractor HeartBeatRequest() {
        return new HeartBeatTimeExtractor();
    }

    public static final class PaymentRequestTimeExtractor implements TimestampExtractor {
        @Override
        public long extract(ConsumerRecord<Object, Object> consumerRecord, long prevTime) {
            PaymentRequest record = (PaymentRequest) consumerRecord.value();
            long eventTime = Instant.parse(record.getCreatedTime()).toEpochMilli();
            return ((eventTime > 0) ? eventTime : prevTime);
        }
    }

    public static PaymentRequestTimeExtractor PaymentRequest() {
        return new PaymentRequestTimeExtractor();
    }

    public static final class PaymentConfirmationTimeExtractor implements TimestampExtractor {

        @Override
        public long extract(ConsumerRecord<Object, Object> consumerRecord, long prevTime) {
            PaymentConfirmation record = (PaymentConfirmation) consumerRecord.value();
            long eventTime = Instant.parse(record.getCreatedTime()).toEpochMilli();
            return ((eventTime > 0) ? eventTime : prevTime);
        }
    }

    public static PaymentConfirmationTimeExtractor PaymentConfirmation() {
        return new PaymentConfirmationTimeExtractor();
    }

}
