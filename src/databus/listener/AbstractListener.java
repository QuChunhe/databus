package databus.listener;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.core.EventFilter;
import databus.core.Listener;
import databus.core.Publisher;

public abstract class AbstractListener implements Listener {
    
    public void setPublisher(Publisher publisher) {
        this.publisher = publisher;
    }
    
    @Override
    public void initialize(Properties properties) {
        topic = properties.getProperty("topic");
    }

    public void onEvent(Event event) {
        if (null != topic) {
            event.topic(topic);
        }        
        for(EventFilter f : filters) {
            if (f.reject(event)) {
                log.info(f.toString()+" reject : "+event.toString());
                return;
            }
        } 
        publisher.publish(event);
    }    
    
    @Override
    public void setFilter(EventFilter filter) {
        if (null == filter) {
            return;
        }
        filters.add(filter);        
    }

    private static Log log = LogFactory.getLog(AbstractListener.class);

    private Publisher publisher;
    private String topic;
    private List<EventFilter> filters = new LinkedList<EventFilter>();
}