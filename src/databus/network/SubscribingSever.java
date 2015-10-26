package databus.network;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map.Entry;

import databus.core.Publisher;
import databus.core.Subscriber;
import databus.event.SubscriptionEvent;
import databus.event.management.SubscriptionEventWrapper;
import databus.util.Configuration;
import databus.util.RemoteTopic;

public class SubscribingSever {    
    
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
            String topic = entry.getKey().toString().tr;
            RemoteTopic remoteTopic = config.parseRemoteTopic(topic);
            if (null == remoteTopic) {
                log.error(key+" cannot be parsed as RemoteTopic!");
                continue;
            }
            
            String value = entry.getValue().toString();
            BatchSubscriber batchSubscriber 
                                       = parse(value, remoteTopic.topic());
            if ((null == batchSubscriber)||(batchSubscriber.size() == 0)) {
                log.error(value+" cannot be parsed as Subcribers");
                continue;
            }
            
            batchSubscribers.put(remoteTopic, batchSubscriber);
        } 
    }

    
    private static Log log = LogFactory.getLog(SubscribingSever.class);
    
    private Publisher publisher;
    private Map<RemoteTopic,BatchSubscriber> batchSubscribers;

}
