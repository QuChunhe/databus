package databus.network;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.core.Receiver;
import databus.event.management.Subscription;
import databus.event.management.Withdrawal;
import databus.util.InternetAddress;
import databus.util.RemoteTopic;

public class Subscriber {

    public Subscriber(Client client) {
        receiversMap = new ConcurrentHashMap<RemoteTopic,Set<Receiver>>();
        this.client = client;
    }

    public void receive(Event event) {
        RemoteTopic key = new RemoteTopic(event.address(), event.topic());
        Set<Receiver> receiversSet = receiversMap.get(key);
        if (null == receiversSet) {
            log.error(key.toString() + " has't been subscribed!");
            Withdrawal withdrawal = new Withdrawal();
            withdrawal.topic(event.topic());
            client.send(withdrawal, event.address());
        } else {
            for (Receiver receiver : receiversSet) {
                receiver.receive(event);
            }
        }
    } 

    public void subscribe() {
        for (RemoteTopic remoteTopic : receiversMap.keySet()) {
            InternetAddress remoteAddress = remoteTopic.remoteAddress();
            Subscription event = new Subscription();
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
    
    public void register(String rawString, Receiver receiver) {
        String[] rawParts = rawString.split("/",2);
        if(rawParts.length != 2) {
            log.error(rawString+" can't be splitted by '/'");
            return;
        }
        
        String[] addressInfo = rawParts[0].split(":");
        if (addressInfo.length != 2) {
            log.error(rawParts[0]+" can't be splitted by ':'");
            return;
        }
        int port = Integer.parseInt(addressInfo[1]);
        InternetAddress netAddress = new InternetAddress(addressInfo[0],port);
        String topic = rawParts[1].replace("/", ":");
        RemoteTopic remoteTopic = new RemoteTopic(netAddress, topic);
        register(remoteTopic, receiver);
    }
    
    public void withdraw(RemoteTopic remoteTopic, Receiver receiver) {
        Set<Receiver> receiversSet = receiversMap.get(remoteTopic);
        if (null == receiversSet) {
           log.error("Don't contain the RemoteTopic "+remoteTopic.toString());
        } else {
            if (!receiversSet.remove(receiver)) {
                log.error("Don't contain the receiver "+receiver.toString());
            } else if (receiversSet.size()==0) {
                receiversMap.remove(remoteTopic);
            }
        }
    }

    private static Log log = LogFactory.getLog(Subscriber.class);

    private Map<RemoteTopic, Set<Receiver>> receiversMap;
    private Client client;
}
