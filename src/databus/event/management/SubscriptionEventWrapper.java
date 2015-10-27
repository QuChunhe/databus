package databus.event.management;

import databus.core.Publisher;
import databus.event.SubscriptionEvent;
import databus.util.InternetAddress;

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
    public void execute(Publisher publisher) {
        InternetAddress remoteAddress = address();
        String topic = topic();
        publisher.subscribe(topic, remoteAddress);        
    }
}
