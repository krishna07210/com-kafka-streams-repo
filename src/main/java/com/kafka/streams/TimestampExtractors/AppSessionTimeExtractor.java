package com.kafka.streams.TimestampExtractors;

import com.kafka.model.UserClicks;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Instant;

public class AppSessionTimeExtractor {
    public class AppTimestampExtractor implements TimestampExtractor {
        @Override
        public long extract(ConsumerRecord<Object, Object> consumerRecord, long prevTime) {
            UserClicks userClicks = (UserClicks) consumerRecord.value();
            Long userClickTime = Instant.parse(userClicks.getCreatedTime()).toEpochMilli();
            return (userClickTime > 0 ? userClickTime : prevTime);
        }
    }
}
