package databus.network;

import databus.core.Event;
import databus.core.Publisher;
import databus.core.Subscriber;

import databus.event.ManagementEvent;
import databus.event.management.SubscriptionEventWrapper;
import databus.util.InternetAddress;
import databus.util.RemoteTopic;

public class SubscribingSever implements Subscriber{    
    
    public SubscribingSever(Publisher publisher) {
        batchSubscriber = new BatchSubscriber();
        this.publisher = publisher;
    }
    
    public void subscribe() {
        for(RemoteTopic remoteTopic : batchSubscriber.getAllRemoteTopic()) {
            InternetAddress remoteAddress = remoteTopic.remoteAddress();
            String topic = remoteTopic.topic();
            ManagementEvent event = new SubscriptionEventWrapper(topic);
            publisher.publish(remoteAddress, event);
        }
    }   

    @Override
    public boolean receive(Event event) {
        if (Event.Source.MANAGEMENT == event.source()) {
            ((ManagementEvent) event).execute(publisher);
            return true;
        }
        
        return batchSubscriber.receive(event);
    }  
    
    private Publisher publisher;
    private BatchSubscriber batchSubscriber;


}
