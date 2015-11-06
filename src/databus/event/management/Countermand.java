package databus.event.management;

import databus.network.Publisher;
import databus.util.InternetAddress;

public class Countermand extends AbstractMgtEvent {

    public Countermand() {
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
