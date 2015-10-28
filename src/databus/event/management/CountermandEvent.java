package databus.event.management;

import databus.core.Publisher;
import databus.util.InternetAddress;

public class CountermandEvent extends AbstractManagementEvent {

    public CountermandEvent(String topic) {
        super(topic);
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
