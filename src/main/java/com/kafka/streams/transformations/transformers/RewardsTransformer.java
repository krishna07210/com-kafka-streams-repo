package com.kafka.streams.transformations.transformers;

import com.kafka.model.PosInvoice;
import com.kafka.model.RewardsNotification;
import com.kafka.streams.common.RewardsAppConstants;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

@SuppressWarnings("unchecked")
public class RewardsTransformer implements ValueTransformer<PosInvoice, RewardsNotification> {
    private KeyValueStore<String, Double> stateStore;

    @Override
    public void init(ProcessorContext processorContext) {
        this.stateStore = (KeyValueStore<String, Double>) processorContext.getStateStore(RewardsAppConstants.REWARDS_STORE_NAME);
    }

    @Override
    public RewardsNotification transform(PosInvoice posInvoice) {
        RewardsNotification notification = new RewardsNotification()
                .withInvoiceNumber(posInvoice.getInvoiceNumber())
                .withCustomerCardNo(posInvoice.getCustomerCardNo())
                .withTotalAmount(posInvoice.getTotalAmount())
                .withEarnedLoyaltyPoints(posInvoice.getTotalAmount() * RewardsAppConstants.LOYALTY_FACTOR)
                .withTotalLoyaltyPoints(0.0);
        Double accumulatedRewards = stateStore.get(notification.getCustomerCardNo());
        Double totalRewards;
        if (accumulatedRewards != null)
            totalRewards = accumulatedRewards + notification.getEarnedLoyaltyPoints();
        else
            totalRewards = notification.getEarnedLoyaltyPoints();
        stateStore.put(notification.getCustomerCardNo(), totalRewards);
        notification.setTotalLoyaltyPoints(totalRewards);
        return notification;
    }

    @Override
    public void close() {
    }
}
