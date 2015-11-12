package databus.event.management;

import databus.core.Event;
import databus.event.AbstractConfirmation;
import databus.event.ManagementEvent;

public class SubscribingConfirmation extends AbstractConfirmation<Subscription>{

    @Override
    public String type() {
        return Event.Source.MANAGEMENT.toString()+":"+
               ManagementEvent.Type.SUBSCRIPTION.toString();
    }

    @Override
    public String topic() {
        return ManagementEvent.Type.SUBSCRIPTION.toString();
    }

    @Override
    public String toString() {
        return "SubscribingConfirmation [toString()=" + super.toString() + "]";
    }    
}
