package com.kafka.streams.utils;

import com.kafka.streams.common.StreamsConstants;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import java.util.ArrayList;
import java.util.List;

public class QueryServer {
    private static final Logger logger = LoggerFactory.getLogger(QueryServer.class);
    private final String NO_RESULTS = "No Results Found";
    private final String APPLICATION_NOT_ACTIVE = "Application is not active. Try later.";
    private final KafkaStreams streams;
    private Boolean isActive = false;
    private final HostInfo hostInfo;
    private Client client;

    public QueryServer(KafkaStreams streams, String hostname, int port) {
        this.streams = streams;
        this.hostInfo = new HostInfo(hostname, port);
        client = ClientBuilder.newClient();
    }

    public void setActive(Boolean state) {
        isActive = state;
    }

    private List<KeyValue<String, String>> readAllFromLocal() {

        List<KeyValue<String, String>> localResults = new ArrayList<>();
        ReadOnlyKeyValueStore<String, String> stateStore =
                streams.store(
                        StreamsConstants.STATE_STORE_NAME,
                        QueryableStoreTypes.keyValueStore()
                );

        stateStore.all().forEachRemaining(localResults::add);
        return localResults;
    }

    public void start() {
        logger.info("Starting Query Server at http://" + hostInfo.host() + ":" + hostInfo.port()
                + "/" + StreamsConstants.STATE_STORE_NAME + "/all");

        Spark.port(hostInfo.port());

        Spark.get("/" + StreamsConstants.STATE_STORE_NAME + "/all", (req, res) -> {

            List<KeyValue<String, String>> allResults;
            String results;

            if (!isActive) {
                results = APPLICATION_NOT_ACTIVE;
            } else {
                allResults = readAllFromLocal();
                results = (allResults.size() == 0) ? NO_RESULTS
                        : allResults.toString();
            }
            return results;
        });
    }

    public void stop() {
        client.close();
        Spark.stop();
    }

}

