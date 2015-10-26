package databus.event.management;

import databus.event.SubscriptionEvent;

public class SubscriptionEventWrapper extends AbstractManagementEvent 
                                      implements SubscriptionEvent{

    public SubscriptionEventWrapper(String topic) {
        super(topic);
    }
    
    public SubscriptionEventWrapper() {
        this(null);
    }

    @Override
    public String type() {
        return Type.SUBSCRIPTION.toString();
    }
}
