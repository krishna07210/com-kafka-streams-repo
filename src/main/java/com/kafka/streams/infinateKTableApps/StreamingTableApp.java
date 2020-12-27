package com.kafka.streams.infinateKTableApps;

import com.kafka.streams.common.CommonServices;
import com.kafka.streams.common.StreamsConstants;
import com.kafka.streams.utils.QueryServer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * State Store Example
 */
public class StreamingTableApp {
    public static void main(String[] args) {
        final String topicName = "stock-tick";
        final Logger logger = LoggerFactory.getLogger(StreamingTableApp.class);
        final Properties streamConfig = CommonServices.getStreamConfiguration("StreamingTableApp");
        streamConfig.put(StreamsConfig.STATE_DIR_CONFIG, StreamsConstants.STATE_STORE_LOCATION);
        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, String> KT0 = builder.table(topicName, Consumed.with(Serdes.String(), Serdes.String()));
        KT0.toStream().print(Printed.<String, String>toSysOut().withLabel("KT0"));

        KTable<String, String> KT1 = KT0.filter((key, value) ->
                        key.matches(StreamsConstants.REG_EX_SYMBOL) && !value.isEmpty(),
                Materialized.as(StreamsConstants.STATE_STORE_NAME));
        KT1.toStream().print(Printed.<String, String>toSysOut().withLabel("KT1"));


        KafkaStreams streams = new KafkaStreams(builder.build(), streamConfig);
        //Query Server
        QueryServer queryServer = new QueryServer(streams, StreamsConstants.QUERY_SERVER_HOST, StreamsConstants.QUERY_SERVER_PORT);
        streams.setStateListener((newState, oldState) -> {
            logger.info("State Changing to " + newState + " from " + oldState);
            queryServer.setActive(newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING);
        });

        streams.start();
        queryServer.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Streams");
            queryServer.stop();
            streams.close();
        }));
    }
}
