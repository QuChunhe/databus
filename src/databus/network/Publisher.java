package databus.network;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
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
            log.info(remoteAddress.toString()+" has been contained");
        } else {
            addresses.add(remoteAddress);
            isAdded = true;
        }
        return isAdded;
    }
    
    public void receive(Event event) {
        // do nothing
    }

    public void publish(Event event) {
        String topic = event.topic();
        Set<InternetAddress> addresses = subscribersMap.get(topic);
        if (null != addresses) {
            client.send(event, addresses);
        } else {
            log.info(event.toString()+" has't subscriber!");
        }
    }

    protected Map<String, Set<InternetAddress>> subscribersMap;
    protected Client client;

    private static Log log = LogFactory.getLog(Publisher.class);    
}
