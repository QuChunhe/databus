package databus.listener;

import databus.core.Event;
import databus.core.Listener;
import databus.core.Publisher;

public abstract class AbstractListener implements Listener {

    @Override
    public void setPublisher(Publisher publisher) {
        this.publisher = publisher;
        publisher.addListener(this);
    }

    public void setTopic(String topic) {
        this.topic = topic;
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