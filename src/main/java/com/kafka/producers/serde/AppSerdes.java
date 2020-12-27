/*
 * Copyright (c) 2019. Prashant Kumar Pandey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.kafka.producers.serde;

import com.kafka.model.*;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.HashMap;
import java.util.Map;

/**
 * Factory class for Serdes
 */

public class AppSerdes extends Serdes {
    static final class PosInvoiceSerde extends Serdes.WrapperSerde<PosInvoice> {
        PosInvoiceSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<PosInvoice> PosInvoice() {
        PosInvoiceSerde serde = new PosInvoiceSerde();
        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, PosInvoice.class);
        serde.configure(serdeConfigs, false);
        return serde;
    }

    static final class NotificationSerde extends Serdes.WrapperSerde<Notification> {
        NotificationSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<Notification> Notification() {
        NotificationSerde serde = new NotificationSerde();
        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, Notification.class);
        serde.configure(serdeConfigs, false);
        return serde;
    }

    static final class RewardsNotificationSerde extends Serdes.WrapperSerde<RewardsNotification> {
        RewardsNotificationSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<RewardsNotification> RewardsNotification() {
        RewardsNotificationSerde serde = new RewardsNotificationSerde();
        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, RewardsNotification.class);
        serde.configure(serdeConfigs, false);
        return serde;
    }

    static final class HadoopRecordSerde extends Serdes.WrapperSerde<HadoopRecord> {
        HadoopRecordSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<HadoopRecord> HadoopRecord() {
        HadoopRecordSerde serde = new HadoopRecordSerde();
        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, HadoopRecord.class);
        serde.configure(serdeConfigs, false);
        return serde;
    }

    static final class EmployeeSerde extends Serdes.WrapperSerde<Employee> {
        EmployeeSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<Employee> EmployeeRecord() {
        EmployeeSerde serde = new EmployeeSerde();
        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, Employee.class);
        serde.configure(serdeConfigs, false);
        return serde;
    }

    static final class DepartmentAggerateSerde extends Serdes.WrapperSerde<DepartmentAggregate> {
        DepartmentAggerateSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<DepartmentAggregate> DepartmentAggerateRecord() {
        DepartmentAggerateSerde serde = new DepartmentAggerateSerde();
        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, DepartmentAggregate.class);
        serde.configure(serdeConfigs, false);
        return serde;
    }

    static final class SimpleInvoiceSerde extends Serdes.WrapperSerde<SimpleInvoice> {
        SimpleInvoiceSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<SimpleInvoice> SimpleInvoiceRecord() {
        SimpleInvoiceSerde serde = new SimpleInvoiceSerde();
        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, SimpleInvoice.class);
        serde.configure(serdeConfigs, false);
        return serde;
    }

    static final class HeartBeatSerde extends Serdes.WrapperSerde<HeartBeat> {
        HeartBeatSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<HeartBeat> HeartBeatRecord() {
        HeartBeatSerde serde = new HeartBeatSerde();
        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, HeartBeat.class);
        serde.configure(serdeConfigs, false);
        return serde;
    }

    static final class UserClicksSerde extends Serdes.WrapperSerde<UserClicks> {
        UserClicksSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<UserClicks> UserClicksRecord() {
        UserClicksSerde serde = new UserClicksSerde();
        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, UserClicks.class);
        serde.configure(serdeConfigs, false);
        return serde;
    }

}
