package databus.network;

import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.core.Receiver;
import databus.event.management.SubscriptionEvent;
import databus.util.Configuration;
import databus.util.InternetAddress;
import databus.util.RemoteTopic;

public class Subscriber implements Receiver{    
    
    public Subscriber(Client client) {
        Configuration config = Configuration.instance();
        subscriberMap = config.loadSubscribers();
        this.client = client;
    }  

    @Override
    public boolean receive(Event event) {
        RemoteTopic key = new RemoteTopic(event.address(),event.topic());
        Set<Receiver> subscribers = subscriberMap.get(key); 
        if (null == subscribers) {
            log.error(key.toString()+" has not been subscribed!");
            return false;
        }
        boolean doSucceed = true;
        for(Receiver s : subscribers) {
            doSucceed = doSucceed && s.receive(event);
        }
        return doSucceed;
    }
    
    public void subscribe() {
        for(RemoteTopic remoteTopic : subscriberMap.keySet()) {
            InternetAddress remoteAddress = remoteTopic.remoteAddress();
            SubscriptionEvent event = new SubscriptionEvent();
            event.topic(remoteTopic.topic());
            client.send(event, remoteAddress);
        }
    }
    
    private static Log log = LogFactory.getLog(Subscriber.class);
    
    private Map<RemoteTopic, Set<Receiver>> subscriberMap;
    private Client client;
}
