package com.kafka.streams.common;

public class RewardsAppConstants {
    public final static String APPLICATION_ID = "RewardsApp";
    public final static String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093";
    public final static String POS_TOPIC_NAME = "pos";
    public final static String NOTIFICATION_TOPIC = "loyalty";
    public final static String CUSTOMER_TYPE_PRIME = "PRIME";
    public final static Double LOYALTY_FACTOR = 0.02;
    public final static String REWARDS_STORE_NAME = "CustomerRewardsStore";
    public final static String REWARDS_TEMP_TOPIC = "CustomerRewardsTempTopic";
}
