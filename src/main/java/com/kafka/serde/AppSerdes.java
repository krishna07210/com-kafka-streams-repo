package com.kafka.serde;

import com.kafka.model.*;
import com.kafka.streams.sorting.Top3NewsTypes;
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


    static final class PaymentRequestSerde extends Serdes.WrapperSerde<PaymentRequest> {
        PaymentRequestSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<PaymentRequest> PaymentRequestRecord() {
        PaymentRequestSerde serde = new PaymentRequestSerde();
        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, PaymentRequest.class);
        serde.configure(serdeConfigs, false);
        return serde;
    }

    static final class PaymentConfirmationSerde extends Serdes.WrapperSerde<PaymentConfirmation> {
        PaymentConfirmationSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<PaymentConfirmation> PaymentConfirmationRecord() {
        PaymentConfirmationSerde serde = new PaymentConfirmationSerde();
        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, PaymentConfirmation.class);
        serde.configure(serdeConfigs, false);
        return serde;
    }

    static final class AdClickSerde extends Serdes.WrapperSerde<AdClick> {
        AdClickSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<AdClick> AdClickRecord() {
        AdClickSerde serde = new AdClickSerde();
        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, AdClick.class);
        serde.configure(serdeConfigs, false);
        return serde;
    }

    static final class AdInventoriesSerde extends Serdes.WrapperSerde<AdInventories> {
        AdInventoriesSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<AdInventories> AdInventoryRecord() {
        AdInventoriesSerde serde = new AdInventoriesSerde();
        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, AdInventories.class);
        serde.configure(serdeConfigs, false);
        return serde;
    }

    static final class UserDetailsSerde extends Serdes.WrapperSerde<UserDetails> {
        UserDetailsSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<UserDetails> UserDetailsRecord() {
        UserDetailsSerde serde = new UserDetailsSerde();
        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, UserDetails.class);
        serde.configure(serdeConfigs, false);
        return serde;
    }


    static final class UserLoginSerde extends Serdes.WrapperSerde<UserLogin> {
        UserLoginSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<UserLogin> UserLoginRecord() {
        UserLoginSerde serde = new UserLoginSerde();
        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, UserLogin.class);
        serde.configure(serdeConfigs, false);
        return serde;
    }

    static final class ClickByNewTypeSerde extends Serdes.WrapperSerde<ClicksByNewsType> {
        ClickByNewTypeSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<ClicksByNewsType> ClicksByNewsTypeRecord() {
        ClickByNewTypeSerde serde = new ClickByNewTypeSerde();
        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, ClicksByNewsType.class);
        serde.configure(serdeConfigs, false);
        return serde;
    }

    static final class Top3NewsTypesSerde extends WrapperSerde<Top3NewsTypes> {
        Top3NewsTypesSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<Top3NewsTypes> Top3NewsTypes() {
        Top3NewsTypesSerde serde = new Top3NewsTypesSerde();
        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, Top3NewsTypes.class);
        serde.configure(serdeConfigs, false);
        return serde;
    }

    static final class AdImpressionSerde extends WrapperSerde<AdImpression> {
        AdImpressionSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }


    public static Serde<AdImpression> AdImpression() {
        AdImpressionSerde serde = new AdImpressionSerde();
        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, AdImpression.class);
        serde.configure(serdeConfigs, false);
        return serde;
    }

    static final class AdClickCTRSerde extends WrapperSerde<AdClickCTR> {
        AdClickCTRSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<AdClickCTR> AdClickCTR() {
        AdClickCTRSerde serde = new AdClickCTRSerde();
        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, AdClickCTR.class);
        serde.configure(serdeConfigs, false);
        return serde;
    }

    static final class CampaignPerformanceSerde extends WrapperSerde<CampaignPerformance> {
        CampaignPerformanceSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<CampaignPerformance> CampaignPerformance() {
        CampaignPerformanceSerde serde = new CampaignPerformanceSerde();
        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, CampaignPerformance.class);
        serde.configure(serdeConfigs, false);
        return serde;
    }
}

