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
        subscribersMap = new ConcurrentHashMap<String,Set<InternetAddress>>();
        this.client = client;
    }

    public void subscribe(String topic, InternetAddress remoteAddress) {
        Set<InternetAddress> addressSet = subscribersMap.get(topic);
        if (null == addressSet) {
            addressSet = new CopyOnWriteArraySet<InternetAddress>();
            addressSet.add(remoteAddress);
            subscribersMap.put(topic, addressSet);
        } else if (addressSet.contains(remoteAddress)){
            log.info(remoteAddress.toString()+" has subscribeed before");
        } else {
            addressSet.add(remoteAddress);
        }
    }
    
    public void unsubscribe(String topic, InternetAddress remoteAddress) {
        Set<InternetAddress> addressSet = subscribersMap.get(topic);
        if (addressSet.remove(remoteAddress)) {
            if (addressSet.isEmpty()) {
                subscribersMap.remove(topic);
            }
        } else {
            log.error(remoteAddress.toString()+" has't subscribe "+topic);
        }
    }

    public void publish(Event event) {
        String topic = event.topic();
        Set<InternetAddress> remoteAddressSet = subscribersMap.get(topic);
        if (null != remoteAddressSet) {
            client.send(event, remoteAddressSet);
        } else {
            log.info(event.toString()+" has't subscriber!");
        }
    }    

    
    private static Log log = LogFactory.getLog(Publisher.class);
    
    private Map<String,Set<InternetAddress>> subscribersMap;    
    private Client client; 
}
