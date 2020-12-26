package com.kafka.producers.producerApps;

import com.kafka.streams.utils.FileUtil;
import com.kafka.streams.common.CommonServices;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

public class FileProducer {
    public static void main(String[] args) throws Exception {
        final String TOPIC_NAME = args[0];
        final String sourceFile = args[1];
        System.out.println("Topic Name :" + TOPIC_NAME);
        System.out.println("Source File : " + sourceFile);
        String status = produceInputData(TOPIC_NAME, sourceFile);
        System.out.println("Status : " + status);
    }

    public static String produceInputData(String topicName, String sourceFile) {
        List<String> inputValues = getInputValue(sourceFile);
        System.out.println("Size : " + inputValues.size());
        try {
            final KafkaProducer<String, String> producer = new KafkaProducer<>(CommonServices.getProducerConfig(), new StringSerializer(), new StringSerializer());
//            producer.send(new ProducerRecord<>(topicName, inputValues.get(0), inputValues.get(0)));
            final Random random = new Random();
            for (String inputvalue : inputValues) {
                producer.send(new ProducerRecord<>(topicName, null, inputvalue));
                System.out.println("Record Produced : " + inputvalue);
                Thread.sleep(1000L);
            }
            return "Data Produced Successfully";
        } catch (Exception e) {
            e.printStackTrace();
            return "Exception Occurred";
        }
    }

    public static List<String> getInputValue(String sourceFile) {
        BufferedReader reader;
        List<String> inputValue = new ArrayList<>();
        try {
            InputStream inputStream = FileUtil.class.getResource("/InputFiles/" + sourceFile).openStream();
            reader = new BufferedReader(new InputStreamReader(inputStream));
            while (reader.ready()) {
                String line = reader.readLine();
                inputValue.add(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return inputValue;
    }
}
