package com.kafka.producers.common;

public class ProducerConstants {
    public final static String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093";
    public final static String TOPIC_NAME = "hello-producer-topic";
    public final static String KAFKA_CONFIG_FILE_LOCATION = "/kafka.properties";
    public final static String[] EVENT_FILES = {"/data/NSE05NOV2018BHAV.csv", "/data/NSE06NOV2018BHAV.csv"};
    public final static String TRANSACTION_ID = "Hello-Producer-Trans";
    public final static String[] SOURCE_TOPIC_NAMES = {"pos"};
    public final static String INVOICE_JSON_FILE = "src/main/resources/data/Invoice.json";
    public final static String PRODUCTS_JSON_FILE = "src/main/resources/data/products.json";
    public final static String ADDRESS_JSON_FILE = "src/main/resources/data/address.json";
}

