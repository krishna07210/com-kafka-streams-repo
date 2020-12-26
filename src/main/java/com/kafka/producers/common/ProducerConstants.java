package com.kafka.producers.common;

public class ProducerConstants {
    public final static String bootstrapServers = "localhost:9092,localhost:9093";
    public final static String topicName = "hello-producer-topic";
    public final static String kafkaConfigFileLocation = "/kafka.properties";
    public final static String[] eventFiles = {"/data/NSE05NOV2018BHAV.csv", "/data/NSE06NOV2018BHAV.csv"};
    public final static String transaction_id = "Hello-Producer-Trans";
    public final static String[] sourceTopicNames = {"pos"};
}

