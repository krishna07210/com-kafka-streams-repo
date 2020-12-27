package com.kafka.streams.TimestampExtractors;

import com.kafka.model.SimpleInvoice;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Instant;

public class InvoiceTimeExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long prevTime) {
        SimpleInvoice simpleInvoice = (SimpleInvoice) consumerRecord.value();
        Long createdTime = Instant.parse(simpleInvoice.getCreatedTime()).toEpochMilli();
        return (createdTime > 0 ? createdTime : prevTime);
    }
}
