package databus.network;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.event.management.SubscribingConfirmation;
import databus.event.management.Subscription;
import databus.event.management.Withdrawal;
import databus.util.InternetAddress;

public class InteractivePublisher extends Publisher{   

    public InteractivePublisher(Client client) {
        super(client);
    }
    
    public void receive(Event event) {
        if (event instanceof Subscription) {
            Subscription e = (Subscription) event;
            InternetAddress address = new InternetAddress(e.ipAddress(), e.port());
            if (address.isValid()){
                subscribe(e.topic(), address);
                confirm(e, address);
            } else {
                log.warn(e.ipAddress()+":"+e.port()+" is a invalid address"); 
            }
                       
        } else if (event instanceof Withdrawal){
            Withdrawal e = (Withdrawal) event;
            InternetAddress address = new InternetAddress(e.ipAddress(), e.port());
            if (address.isValid()){
                withdraw(e.topic(), address);
            }else {
                log.warn(e.ipAddress()+":"+e.port()+" is a invalid address"); 
            }
        }        
    }
    
    public void confirm(Subscription e, InternetAddress remoteAddress) {
        SubscribingConfirmation event = new SubscribingConfirmation();
        event.setConfirmedEvent(e);
        client.send(event, remoteAddress);
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

    private static Log log = LogFactory.getLog(InteractivePublisher.class);
}
