package databus.event.management;

import databus.network.Publisher;
import databus.util.InternetAddress;

public class SubscriptionEvent extends AbstractManagementEvent {

    public SubscriptionEvent() {
        super();
        // TODO Auto-generated constructor stub
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
