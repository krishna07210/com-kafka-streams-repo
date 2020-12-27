package com.kafka.streams.requirements;

import com.kafka.streams.common.CommonServices;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ResetStreams {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ResetStreams.class);
        final StreamsBuilder builder = new StreamsBuilder();
        final Properties streamConfiguration = CommonServices.getStreamConfiguration("ResetStreams");
    }
}
