package com.kafka.java.streams;

import com.kafka.model.AdClickCTR;
import com.kafka.model.AdImpression;
import com.kafka.model.CampaignPerformance;
import com.kafka.serde.AppSerdes;
import com.kafka.streams.topologies.CTRTopology;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CTRTopologyTest {
    private static TopologyTestDriver topologyTestDriver;

    @BeforeAll
    static void setUpAll() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, StreamsConstantsTest.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, StreamsConstantsTest.bootstrapServers);
        props.put(StreamsConfig.STATE_DIR_CONFIG, StreamsConstantsTest.stateStoreLocationUT);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        CTRTopology.withBuilder(streamsBuilder);
        Topology topology = streamsBuilder.build();

        topologyTestDriver = new TopologyTestDriver(topology, props);
    }

    @Test
    @Order(1)
    @DisplayName("Test the impression flow from the source topic to the final output topic.")
    void impressionFlowTest() {
        AdImpression impression = new AdImpression().withImpressionID("100001").withCampaigner("ABC Ltd");
        ConsumerRecordFactory<String, AdImpression> impressionFactory = new ConsumerRecordFactory<>(
                AppSerdes.String().serializer(), AppSerdes.AdImpression().serializer()
        );

        topologyTestDriver.pipeInput(impressionFactory.create(
                StreamsConstantsTest.impressionTopic,
                "100001",
                impression
        ));

        ProducerRecord<String, CampaignPerformance> record = topologyTestDriver.readOutput(
                StreamsConstantsTest.outputTopic,
                AppSerdes.String().deserializer(),
                AppSerdes.CampaignPerformance().deserializer());

        assertAll(
                () -> assertEquals("ABC Ltd", record.value().getCampaigner()),
                () -> assertEquals("1", record.value().getAdImpressions().toString())
        );

    }

    @Test
    @Order(2)
    @DisplayName("Test the impression count increments correctly.")
    void impressionCountTest() {

        AdImpression adImpression = new AdImpression().withImpressionID("100002").withCampaigner("ABC Ltd");
        ConsumerRecordFactory<String, AdImpression> impressionFactory = new ConsumerRecordFactory<>(
                AppSerdes.String().serializer(), AppSerdes.AdImpression().serializer()
        );

        topologyTestDriver.pipeInput(impressionFactory.create(
                StreamsConstantsTest.impressionTopic,
                "100002",
                adImpression
        ));

        ProducerRecord<String, CampaignPerformance> record = topologyTestDriver.readOutput(
                StreamsConstantsTest.outputTopic,
                AppSerdes.String().deserializer(),
                AppSerdes.CampaignPerformance().deserializer()
        );

        assertAll(
                () -> assertEquals("ABC Ltd", record.value().getCampaigner()),
                () -> assertEquals("2", record.value().getAdImpressions().toString())
        );
    }

    @Test
    @Order(3)
    @DisplayName("Test the adClick count increments correctly")
    void adClicksTest() {
        AdClickCTR adClickCTR = new AdClickCTR().withImpressionID("100001").withCampaigner("ABC Ltd");
        ConsumerRecordFactory<String, AdClickCTR> adClickFactory = new ConsumerRecordFactory<>(
                AppSerdes.String().serializer(), AppSerdes.AdClickCTR().serializer());

        topologyTestDriver.pipeInput(adClickFactory.create(
                StreamsConstantsTest.clicksTopic,
                "100001",
                adClickCTR
        ));

        ProducerRecord<String, CampaignPerformance> record = topologyTestDriver.readOutput(
                StreamsConstantsTest.outputTopic,
                AppSerdes.String().deserializer(),
                AppSerdes.CampaignPerformance().deserializer()
        );

        assertAll(
                () -> assertEquals("ABC Ltd", record.value().getCampaigner()),
                () -> assertEquals("2", record.value().getAdImpressions().toString()),
                () -> assertEquals("1", record.value().getAdClicks().toString())
        );
    }

    @Test
    @Order(4)
    @DisplayName("Test the state store holds the correct state")
    void stateStoreTest() {
        KeyValueStore<String, CampaignPerformance> store = topologyTestDriver.getKeyValueStore(StreamsConstantsTest.stateStoreNameCP);
        CampaignPerformance cpValue = store.get("ABC Ltd");

        assertAll(
                () -> assertEquals("ABC Ltd", cpValue.getCampaigner()),
                () -> assertEquals("2", cpValue.getAdImpressions().toString()),
                () -> assertEquals("1", cpValue.getAdClicks().toString())
        );

    }


    @AfterAll
    static void cleanUpAll() throws IOException {
        try {
            topologyTestDriver.close();
        } catch (Exception e) {
            FileUtils.deleteDirectory(new File(StreamsConstantsTest.stateStoreLocationUT));
        }
    }
}
