package com.kafka.producers.datagenerator;

import com.kafka.model.RewardsNotification;
import com.kafka.model.PosInvoice;
import com.kafka.streams.common.RewardsAppConstants;

public class RewardsNotifications {
    public static RewardsNotification getNotificationFrom(PosInvoice invoice) {
        return new RewardsNotification()
                .withInvoiceNumber(invoice.getInvoiceNumber())
                .withCustomerCardNo(invoice.getCustomerCardNo())
                .withTotalAmount(invoice.getTotalAmount())
                .withEarnedLoyaltyPoints(invoice.getTotalAmount() * RewardsAppConstants.LOYALTY_FACTOR)
                .withTotalLoyaltyPoints(invoice.getTotalAmount() * RewardsAppConstants.LOYALTY_FACTOR);
    }
}

