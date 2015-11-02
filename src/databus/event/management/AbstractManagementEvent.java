package databus.event.management;

import databus.event.AbstractEvent;
import databus.event.ManagementEvent;

public abstract class AbstractManagementEvent extends AbstractEvent
                                              implements ManagementEvent{   
    
    public AbstractManagementEvent() {
        time(System.currentTimeMillis());
    }

    @Override
    public Source source() {
        return Source.MANAGEMENT;
    }
    
    @Override
    public String topic() {
        return topic;
    }
    
    public AbstractManagementEvent topic(String topic) {
        this.topic = topic;
        return this;
    }
  
    private String topic;    
}
