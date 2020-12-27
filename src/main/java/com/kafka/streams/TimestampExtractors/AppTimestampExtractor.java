package com.kafka.streams.TimestampExtractors;

import com.kafka.model.HeartBeat;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Instant;

public class AppTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long prevTime) {
        HeartBeat heartBeat = (HeartBeat) consumerRecord.value();
        Long heartBeatTime = Instant.parse(heartBeat.getCreatedTime()).toEpochMilli();
        return (heartBeatTime>0 ? heartBeatTime :prevTime);
    }
}
