package databus.event.management;

import databus.core.Publisher;
import databus.util.InternetAddress;

public class SubscriptionEvent extends AbstractManagementEvent {

    public SubscriptionEvent(String topic) {
        super(topic);
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
