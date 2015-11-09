package databus.network;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.util.Backup;
import databus.util.InternetAddress;

public class Publisher{   
    
    public Publisher(Client client) {        
        this(client, true);        
    }

    public Publisher(Client client, boolean hasBackup) {
        this.client = client;
        subscribersMap = new ConcurrentHashMap<String, Set<InternetAddress>>();
        this.hasBackup = hasBackup;
        recoverSubscribersMap();
    }

    public void subscribe(String topic, InternetAddress remoteAddress) {
        Set<InternetAddress> addresses = subscribersMap.get(topic);
        if (null == addresses) {
            addresses = new CopyOnWriteArraySet<InternetAddress>();            
            subscribersMap.put(topic, addresses);
        }
        if (addresses.contains(remoteAddress)){
            log.info(remoteAddress.toString()+" has subscribeed before");
        } else {
            addresses.add(remoteAddress);
            restoreSubscribersMap();
        }
    }
    
    public void withdraw(String topic, InternetAddress remoteAddress) {
        Set<InternetAddress> addresses = subscribersMap.get(topic);
        if (addresses.remove(remoteAddress)) {
            if (addresses.isEmpty()) {
                subscribersMap.remove(topic);
            }
        } else {
            log.error(remoteAddress.toString()+" has't subscribe "+topic);
        }
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

    private void recoverSubscribersMap() {
        if (!hasBackup) {
            return;
        }
     
        Map<String, Set<InternetAddress>> copy = 
                        Backup.instance().restore(BACKUP_NAME, subscribersMap);
        if (null != copy) {
            subscribersMap = copy;
            log.info(subscribersMap.toString()+ " has recovered");
        }       
    }
    
    private void restoreSubscribersMap() {
        if (!hasBackup) {
            return;
        }
        Backup.instance().store(BACKUP_NAME, subscribersMap);
        log.info(subscribersMap.toString()+" has stored");
    }
    
    private static String BACKUP_NAME = "publisher.receivedSubscribers";
    private static Log log = LogFactory.getLog(Publisher.class);
    
    private Map<String, Set<InternetAddress>> subscribersMap;    
    private Client client;
    private boolean hasBackup;
}
