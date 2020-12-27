package com.kafka.streams.aggregators;

import com.kafka.model.DepartmentAggregate;
import com.kafka.serde.AppSerdes;
import com.kafka.streams.common.CommonServices;
import com.kafka.streams.common.RewardsAppConstants;
import com.kafka.streams.transformations.stateless.GroupByTransformation;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Aggregation Using KTable
 * In the DepartmentStatsKSteamApp we have problem in the Aggregations in case Employee Department shifts.
 * We solve this problem using KTable Scenario.
 */

public class DepartmentStatsKTableApp {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(GroupByTransformation.class);
        final String topicName = "employee";
        final Properties streamConfig = CommonServices.getStreamConfigurationNoSerdes("department-salary-app");
        StreamsBuilder builder = new StreamsBuilder();

        builder.table(topicName,
                Consumed.with(AppSerdes.String(), AppSerdes.EmployeeRecord()))
                .groupBy((k, v) -> KeyValue.pair(v.getDepartment(), v), Grouped.with(AppSerdes.String(), AppSerdes.EmployeeRecord()))
                .aggregate(
                        //Initializer
                        () -> new DepartmentAggregate()
                                .withEmployeeCount(0)
                                .withTotalSalary(0)
                                .withAvgSalary(0D),
                        //Adder
                        (k, v, aggValue) -> new DepartmentAggregate()
                                .withEmployeeCount(aggValue.getEmployeeCount() + 1)
                                .withTotalSalary(aggValue.getTotalSalary() + v.getSalary())
                                .withAvgSalary((aggValue.getTotalSalary() + v.getSalary()) /
                                        (aggValue.getEmployeeCount() + 1D)),
                        //subtractor
                        (k, v, aggValue) -> new DepartmentAggregate()
                                .withEmployeeCount(aggValue.getEmployeeCount() - 1)
                                .withTotalSalary(aggValue.getTotalSalary() - v.getSalary())
                                .withAvgSalary((aggValue.getTotalSalary() - v.getSalary()) /
                                        (aggValue.getEmployeeCount() + 1D)),
                        //Serializer
                        Materialized.<String, DepartmentAggregate, KeyValueStore<Bytes, byte[]>>
                                as(RewardsAppConstants.REWARDS_STORE_NAME)
                                .withKeySerde(AppSerdes.String())
                                .withValueSerde(AppSerdes.DepartmentAggerateRecord())
                ).toStream().foreach((key, deptAggRec) -> System.out.println(key +
                "=> Count::" + deptAggRec.getEmployeeCount() + " Total:: " +
                deptAggRec.getTotalSalary() + " Average:: " +
                deptAggRec.getAvgSalary()));
        KafkaStreams streams = new KafkaStreams(builder.build(), streamConfig);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Streams");
            streams.close();
        }));

    }
}

