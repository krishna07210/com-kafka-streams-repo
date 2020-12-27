package com.kafka.streams.custompartitioners;

import com.kafka.model.PosInvoice;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class RewardsCustomPartitioner implements StreamPartitioner<String, PosInvoice> {

    @Override
    public Integer partition(String topic, String key, PosInvoice value, int numPartitions) {
        return value.getCustomerCardNo().hashCode()%numPartitions;
    }
}
