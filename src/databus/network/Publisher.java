package databus.network;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.event.management.SubscribingConfirmation;
import databus.event.management.Subscription;
import databus.event.management.Withdrawal;
import databus.util.InternetAddress;

public class Publisher{   

    public Publisher(Client client) {
        this.client = client;
        subscribersMap = new ConcurrentHashMap<String, Set<InternetAddress>>();
    }
    
    public boolean subscribe(String topic, InternetAddress remoteAddress) {
        boolean isAdded = false;
        Set<InternetAddress> addresses = subscribersMap.get(topic);
        if (null == addresses) {
            addresses = new CopyOnWriteArraySet<InternetAddress>();            
            subscribersMap.put(topic, addresses);
        }
        if (addresses.contains(remoteAddress)){
            log.info(remoteAddress.toString()+" has subscribed before");
        } else {
            addresses.add(remoteAddress);
            isAdded = true;
        }
        return isAdded;
    }
    
    public void receive(Event event) {
        if (event instanceof Subscription) {
            subscribe(event.topic(), event.address());   
            confirm((Subscription)event);
        } else if (event instanceof Withdrawal){
            withdraw(event.topic(), event.address());
        }        
    }
    
    public void confirm(Subscription e) {
        SubscribingConfirmation event = new SubscribingConfirmation();
        event.setConfirmedEvent(e);
        client.send(event, e.address());
    }
    
    public boolean withdraw(String topic, InternetAddress remoteAddress) {
        boolean isRemoved = true;
        Set<InternetAddress> addresses = subscribersMap.get(topic);
        if ((null != addresses) && (addresses.remove(remoteAddress))) {
            if (addresses.isEmpty()) {
                subscribersMap.remove(topic);
            }
            isRemoved = true;
        } else {
            log.error(remoteAddress.toString()+" has't subscribe "+topic);
        }
        return isRemoved;
    }

    public void publish(Event event) {
        String topic = event.topic();
        log.info("topic:"+topic);
        Set<InternetAddress> addresses = subscribersMap.get(topic);
        log.info(subscribersMap.toString());
        if (null != addresses) {
            client.send(event, addresses);
        } else {
            log.info(event.toString()+" has't subscriber!");
        }
    }
    
    protected Map<String, Set<InternetAddress>> subscribersMap; 

    private static Log log = LogFactory.getLog(Publisher.class);
       
    private Client client;
}
