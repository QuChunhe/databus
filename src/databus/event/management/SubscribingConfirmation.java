package databus.event.management;

import databus.core.Event;
import databus.event.AbstractConfirmation;
import databus.event.ManagementEvent;
import databus.network.Subscriber;
import databus.util.RemoteTopic;

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
    
    public void execute(Subscriber subscriber) {
        Subscription event = getConfirmedEvent();
        RemoteTopic remoteTopic = new RemoteTopic(event.address(), event.topic());
        subscriber.confirmSubscription(remoteTopic);        
    }
 
}
