package com.kafka.producers.producerApps;

import com.kafka.streams.utils.FileUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class Dispatcher implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Dispatcher.class);
    private String fileLocation;
    private String topicName;
    private KafkaProducer<Integer, String> producer;

    Dispatcher(KafkaProducer<Integer, String> producer, String topicName, String fileLocation) {
        this.producer = producer;
        this.topicName = topicName;
        this.fileLocation = fileLocation;
    }

    @Override
    public void run() {
        logger.info("Start Processing " + fileLocation);
        BufferedReader reader;
        int counter = 0;
        try {
            InputStream inputStream = FileUtil.class.getResource(fileLocation).openStream();
            reader = new BufferedReader(new InputStreamReader(inputStream));
            while (reader.ready()) {
                String line = reader.readLine();
                producer.send(new ProducerRecord<>(topicName, null, line));
                counter++;
            }
            logger.info("Finished Sending " + counter + " messages from " + fileLocation);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}

