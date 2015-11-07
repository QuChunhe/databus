package databus.event.management;

import databus.network.Publisher;
import databus.util.InternetAddress;

public class Withdrawal extends AbstractMgtEvent {

    public Withdrawal() {
        super();
    }

    @Override
    public String type() {
        return Type.WITHDRAWAL.toString();
    }

    @Override
    public void execute(Publisher publisher) {
        InternetAddress remoteAddress = address();
        String topic = topic();
        publisher.withdraw(topic, remoteAddress);
    }
}
