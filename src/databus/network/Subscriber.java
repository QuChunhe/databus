package databus.network;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.core.Receiver;
import databus.event.management.SubscriptionEvent;
import databus.util.InternetAddress;
import databus.util.RemoteTopic;

public class Subscriber {

    public Subscriber(Client client) {
        receiversMap = new ConcurrentHashMap<RemoteTopic,Set<Receiver>>();
        this.client = client;
    }

     public void receive(Event event) {
        RemoteTopic key = new RemoteTopic(event.address(), event.topic());
        Set<Receiver> subscribers = receiversMap.get(key);
        if (null == subscribers) {
            log.error(key.toString() + " has not been subscribed!");
        } else {
            for (Receiver s : subscribers) {
                s.receive(event);
            }
        }
    } 

    public void subscribe() {
        for (RemoteTopic remoteTopic : receiversMap.keySet()) {
            InternetAddress remoteAddress = remoteTopic.remoteAddress();
            SubscriptionEvent event = new SubscriptionEvent();
            event.topic(remoteTopic.topic());
            client.send(event, remoteAddress);
        }
    }
    
    public void register(RemoteTopic remoteTopic, Receiver receiver) {
        Set<Receiver> receiversSet = receiversMap.get(remoteTopic);
        if (null == receiversSet) {
            receiversSet = new CopyOnWriteArraySet<Receiver>();
            receiversMap.put(remoteTopic, receiversSet);
        }
        receiversSet.add(receiver);
    }
    
    public void register(String topicString, Receiver receiver) {
        String[] topicParts = topicString.split("/",2);
        if(topicParts.length != 2) {
            log.error(topicString+" cannot be splitted by '/'");
            return;
        }
        
        String[] addressInfo = topicParts[0].split(":");
        if (addressInfo.length != 2) {
            log.error(topicParts[0]+" cannot be splitted by ':'");
            return;
        }
        int port = Integer.parseInt(addressInfo[1]);
        InternetAddress netAddress = new InternetAddress(addressInfo[0],port);

        RemoteTopic remoteTopic = new RemoteTopic(netAddress, topicParts[1]);
        register(remoteTopic, receiver);
    }
    
    public void unregister(RemoteTopic remoteTopic, Receiver receiver) {
        Set<Receiver> receiversSet = receiversMap.get(remoteTopic);
        if (null == receiversSet) {
           log.error("Donot contain the RemoteTopic "+remoteTopic.toString());
        } else {
            if (!receiversSet.remove(receiver)) {
                log.error("Donot contain the receiver "+receiver.toString());
            }
        }
    }

    private static Log log = LogFactory.getLog(Subscriber.class);

    private Map<RemoteTopic, Set<Receiver>> receiversMap;
    private Client client;
}
