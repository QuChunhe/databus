package databus.listener;

import databus.core.Listener;
import databus.network.Publisher;

public abstract class AbstractListener implements Listener{
    
    public void setPublisher(Publisher publisher) {
        this.publisher = publisher;
    }

    protected Publisher publisher;
}
