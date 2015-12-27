package databus.network;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.event.management.SubscribingConfirmation;
import databus.event.management.Subscription;
import databus.event.management.Withdrawal;
import databus.util.InternetAddress;

public class SubscribablePublisher extends Publisher{   

    public SubscribablePublisher(Client client) {
        super(client);
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

    private static Log log = LogFactory.getLog(SubscribablePublisher.class);
}
