package databus.event.management;

import databus.network.Publisher;
import databus.util.InternetAddress;

public class CountermandEvent extends AbstractManagementEvent {

    public CountermandEvent() {
        super();
    }

    @Override
    public String type() {
        return Type.COUNTERMAND.toString();
    }

    @Override
    public void execute(Publisher publisher) {
        InternetAddress remoteAddress = address();
        String topic = topic();
        publisher.unsubscribe(topic, remoteAddress);
    }
}
