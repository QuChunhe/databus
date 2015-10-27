package databus.subscriber;

import databus.core.Event;
import databus.core.Subscriber;

public class DefaultSubscriber implements Subscriber{

    @Override
    public boolean receive(Event event) {
        System.out.println(event.toString());
        return true;
    }

}
