package databus.network;

import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.core.Publisher;
import databus.core.Subscriber;

import databus.event.ManagementEvent;
import databus.event.management.SubscriptionEventWrapper;
import databus.util.Configuration;
import databus.util.InternetAddress;
import databus.util.RemoteTopic;

public class SubscribingSever implements Subscriber, Startable{    
    
    public SubscribingSever(Publisher publisher) {
        subscriberMap = Configuration.instance().loadSubscribers();
        this.publisher = publisher;
        server = new Server(this);
    }  

    @Override
    public boolean receive(Event event) {
        if (Event.Source.MANAGEMENT == event.source()) {
            ((ManagementEvent) event).execute(publisher);
            return true;
        }        
        return receive0(event);
    }    
    
    @Override
    public void start() {
                
    }

    @Override
    public boolean isRunning() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void stop() {
        // TODO Auto-generated method stub
        
    }
    
    private void subscribe() {
        for(RemoteTopic remoteTopic : subscriberMap.keySet()) {
            InternetAddress remoteAddress = remoteTopic.remoteAddress();
            String topic = remoteTopic.topic();
            ManagementEvent event = new SubscriptionEventWrapper(topic);
            publisher.publish(remoteAddress, event);
        }
    } 
    
    private boolean receive0(Event event) {
        RemoteTopic key = new RemoteTopic(event.address(),event.topic());
        Set<Subscriber> subscribers = subscriberMap.get(key); 
        if (null == subscribers) {
            log.error(key.toString()+" has not been subscribed!");
            return false;
        }
        boolean doSucceed = true;
        for(Subscriber s : subscribers) {
            doSucceed = doSucceed && s.receive(event);
        }
        return doSucceed;
    }
    
    private static Log log = LogFactory.getLog(SubscribingSever.class);
    private Publisher publisher;
    private Server server;
    private Map<RemoteTopic, Set<Subscriber>> subscriberMap;
    
}
