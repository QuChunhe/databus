package databus.listener;

import java.util.Properties;

import databus.core.*;

public abstract class AbstractListener implements Listener {

    @Override
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
        publisher.publish(event);
    }

    private Publisher publisher;
    private String topic = null;
}