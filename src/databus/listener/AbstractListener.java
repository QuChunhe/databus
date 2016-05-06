package databus.listener;

import databus.core.Event;
import databus.core.Listener;
import databus.network.Publisher;

public abstract class AbstractListener implements Listener {
    
    public void setPublisher(Publisher publisher) {
        this.publisher = publisher;
    }   
    
    public void onEvent(Event event) {
        publisher.publish(event);
    }    
    
    private Publisher publisher;
}