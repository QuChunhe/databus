package databus.network;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.event.management.SubscribingConfirmation;
import databus.event.management.Subscription;
import databus.event.management.Withdrawal;

public class InteractivePublisher extends Publisher{   

    public InteractivePublisher(Client client) {
        super(client);
    }
    
    public void receive(Event event) {
        if (event instanceof Subscription) {
            Subscription e = (Subscription) event;
            SocketAddress address = new InetSocketAddress(e.ipAddress(), e.port());
            subscribe(e.topic(), address);
            confirm(e, address);
        } else if (event instanceof Withdrawal){
            Withdrawal e = (Withdrawal) event;
            SocketAddress address = new InetSocketAddress(e.ipAddress(), e.port());
            withdraw(e.topic(), address);
            
        }        
    }
    
    public void confirm(Subscription e, SocketAddress remoteAddress) {
        SubscribingConfirmation event = new SubscribingConfirmation();
        event.setConfirmedEvent(e);
        client.send(event, remoteAddress);
    }
    
    public boolean withdraw(String topic, SocketAddress remoteAddress) {
        boolean isRemoved = true;
        Set<SocketAddress> addresses = subscribersMap.get(topic);
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
