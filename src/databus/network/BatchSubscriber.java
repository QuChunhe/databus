package databus.network;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.core.Subscriber;
import databus.util.Configuration;
import databus.util.RemoteTopic;

public class BatchSubscriber implements Subscriber{

    public BatchSubscriber() {
        initiate();
    }

    @Override
    public boolean receive(Event event) {
        RemoteTopic key = new RemoteTopic(event.address(),event.topic());
        Set<Subscriber> subscribers = subscriberMap.get(key); 
        if (null == subscribers) {
            log.error(key.toString()+" has not been subscribed!");
            return false;
        }
        boolean doSucceed = true;
        for(Subscriber s : subscribers) {
            doSucceed = doSucceed && s.receive(event);
        }
        return doSucceed;
    }
    
    public Set<RemoteTopic> getAllRemoteTopic() {
        return subscriberMap.keySet();
    }
    
    private void initiate() {
        subscriberMap = new ConcurrentHashMap<RemoteTopic,Set<Subscriber>>();
        
        Configuration config = Configuration.instance();
        Properties properties = config.loadSubscribersProperties();
        
        for(Entry<Object, Object> entry : properties.entrySet()) {
            String key = entry.getKey().toString();
            RemoteTopic remoteTopic = config.parseRemoteTopic(key);
            if (null == remoteTopic) {
                log.error(key+" cannot be parsed as RemoteTopic!");
                continue;
            }
            
            String value = entry.getValue().toString();
            Collection<Subscriber> subscribers = config.parseSubscribers(value);
            if((null == subscribers) || (subscribers.size()==0)) {
                log.error(value+" cannot be parsed as Subcribers");
                continue;
            }            
            
            Set<Subscriber> subscriberSet = 
                              new CopyOnWriteArraySet<Subscriber>(subscribers);
            subscriberMap.put(remoteTopic, subscriberSet);
        } 
    }
    
    private static Log log = LogFactory.getLog(BatchSubscriber.class);
    private Map<RemoteTopic, Set<Subscriber>> subscriberMap;
}
