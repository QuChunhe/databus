package databus.network;

import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;

public class Publisher{    

    public Publisher(Client client) {
        this.client = client;
        subscribersMap = new ConcurrentHashMap<String, Set<SocketAddress>>();
    }
    
    public boolean subscribe(String topic, SocketAddress subscriber) {
        boolean hasAdded = false;
        Set<SocketAddress> addresses = subscribersMap.get(topic);
        if (null == addresses) {
            addresses = new CopyOnWriteArraySet<SocketAddress>();            
            subscribersMap.put(topic, addresses);
        }
        if (addresses.contains(subscriber)){
            log.info(subscriber.toString()+" has been contained");
        } else {
            addresses.add(subscriber);
            hasAdded = true;
        }
        return hasAdded;
    }
    
    public void receive(Event event) {
        // do nothing
        log.info("Has received "+event.toString());
    }

    public void publish(Event event) {
        String topic = event.topic();
        Set<SocketAddress> addresses = subscribersMap.get(topic);
        if (null != addresses) {
            client.send(event, addresses);
        } else {
            log.info(event.toString()+" has't any subscriber!");
        }
    }

    protected Map<String, Set<SocketAddress>> subscribersMap;
    protected Client client;

    private static Log log = LogFactory.getLog(Publisher.class);    
}
