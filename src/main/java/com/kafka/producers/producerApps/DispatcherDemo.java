package com.kafka.producers.producerApps;

import com.kafka.producers.common.ProducerConstants;
import com.kafka.streams.utils.FileUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

public class DispatcherDemo {
    private static final Logger logger = LoggerFactory.getLogger(DispatcherDemo.class);

    public static void main(String[] args) {
        final String topicName = "nse-eod-topic";
        final String applicationId ="dispatcher-demo";
        Properties props = new Properties();
        try {
            System.out.println("Start Dispatcher Demo");
            InputStream inputStream =
                    FileUtil.class.getResource(ProducerConstants.kafkaConfigFileLocation).openStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            while (reader.ready()) {
                String line = reader.readLine();
                logger.info("Kafka Properties loaded :: {} ", line );
            }
//            props.load(inputStream);
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092,localhost:9093");
            props.put(ProducerConfig.CLIENT_ID_CONFIG, applicationId);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        } catch (IOException e) {
            logger.error("Exception Occurred :: {} ",e);
            throw new RuntimeException(e);
        }

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
        Thread[] dispatchers = new Thread[ProducerConstants.eventFiles.length];
        logger.info("Starting Dispatcher threads...");
        for (int i = 0; i < ProducerConstants.eventFiles.length; i++) {
            logger.info("Start the Thread for File :: {} ", ProducerConstants.eventFiles[i]);
            dispatchers[i] = new Thread(new Dispatcher(producer, topicName, ProducerConstants.eventFiles[i]));
            dispatchers[i].start();
        }
        try {
            for (Thread t : dispatchers) t.join();
        } catch (InterruptedException e) {
            logger.error("Main Thread Interrupted");
        } finally {
            producer.close();
            logger.info("Finished Dispatcher Demo");
        }
    }
}

