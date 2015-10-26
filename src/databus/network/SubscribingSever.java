package databus.network;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map.Entry;

import databus.core.Event;
import databus.core.Publisher;
import databus.core.Subscriber;
import databus.event.SubscriptionEvent;
import databus.event.management.SubscriptionEventWrapper;
import databus.util.Configuration;
import databus.util.RemoteTopic;

public class SubscribingSever implements Subscriber{    
    
    public SubscribingSever(Publisher publisher) {
        batchSubscribers = new ConcurrentHashMap<RemoteTopic,BatchSubscriber>();
        this.publisher = publisher;
        initiate();
    }
    
    public void subscribe() {
        for(RemoteTopic remoteTopic : batchSubscribers.keySet()) {
            SubscriptionEvent event = 
                    new SubscriptionEventWrapper(remoteTopic.topic());
            publisher.publish(remoteTopic.remoteAddress(), event);
        }
    }
        
    private void initiate() {
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
            BatchSubscriber batchSubscriber = new BatchSubscriber(subscribers);
                        
            batchSubscribers.put(remoteTopic, batchSubscriber);
        } 
    }

    @Override
    public boolean receive(Event event) {
        // TODO Auto-generated method stub
        return false;
    }
    
    
    private static Log log = LogFactory.getLog(SubscribingSever.class);
    
    private Publisher publisher;
    private Map<RemoteTopic,BatchSubscriber> batchSubscribers;


}
