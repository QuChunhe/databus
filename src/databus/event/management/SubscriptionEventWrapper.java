package databus.event.management;

import databus.core.Publisher;
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

    @Override
    public void execute(Publisher pulisher) {
        // TODO Auto-generated method stub
        
    }
}
