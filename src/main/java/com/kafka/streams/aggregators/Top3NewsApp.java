package com.kafka.streams.aggregators;

import com.kafka.model.UserDetails;
import com.kafka.model.UserLogin;
import com.kafka.serde.AppSerdes;
import com.kafka.streams.common.CommonServices;
import com.kafka.streams.windowStreamsApps.TimestampWindowExtractApp;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Top3NewsApp {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(TimestampWindowExtractApp.class);
        final String userMasterTopic = "user-master";
        final String lastLoginTopic = "user-login";
        final Properties streamConfig = CommonServices.getStreamConfigurationNoSerdes("UserLoginClickCountApp");
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KTable<String, UserDetails> KT0 = streamsBuilder.table(userMasterTopic,
                Consumed.with(AppSerdes.String(), AppSerdes.UserDetailsRecord())
        );
        KTable<String, UserLogin> KT1 = streamsBuilder.table(lastLoginTopic,
                Consumed.with(AppSerdes.String(), AppSerdes.UserLoginRecord())
        );

        KT0.join(KT1, (kt0Val, kt1Val) -> {
                    kt0Val.setLastLogin(kt1Val.getCreatedTime());
                    return kt0Val;
                }
        ).toStream().print(Printed.toSysOut());

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), streamConfig);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Streams");
            streams.close();
        }));
    }
}
