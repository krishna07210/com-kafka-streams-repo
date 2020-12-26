package com.kafka.producers.producerApps;

import com.kafka.producers.common.ProducerConstants;
import com.kafka.producers.serde.JsonSerializer;
import com.kafka.model.PosInvoice;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PosSimulator {
    private static final Logger logger = LoggerFactory.getLogger(PosSimulator.class);

    public static void main(String[] args) {
        final String applicationId = "PosSimulator";
//        if (args.length < 3) {
//            System.out.println("Please provide command line arguments: topicName noOfProducers produceSpeed");
//            System.exit(-1);
//        }
        String topicName = "pos"; //args[0];
        int noOfProducers = 2; //new Integer(args[1]);
        int produceSpeed = 3; //new Integer(args[2]);

        Properties properties = new Properties();
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, applicationId);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ProducerConstants.bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        KafkaProducer<String, PosInvoice> kafkaProducer = new KafkaProducer<>(properties);
        ExecutorService executor = Executors.newFixedThreadPool(noOfProducers);
        final List<RunnableProducer> runnableProducers = new ArrayList<>();

        for (int i = 0; i < noOfProducers; i++) {
            RunnableProducer runnableProducer = new RunnableProducer(i, kafkaProducer, topicName, produceSpeed);
            runnableProducers.add(runnableProducer);
            executor.submit(runnableProducer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (RunnableProducer p : runnableProducers) p.shutdown();
            executor.shutdown();
            logger.info("Closing Executor Service");
            try {
                executor.awaitTermination(produceSpeed * 2, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));

    }
}
