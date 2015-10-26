package databus.event.management;

import databus.event.AbstractEvent;
import databus.event.ManagementEvent;

public abstract class AbstractManagementEvent extends AbstractEvent
                                              implements ManagementEvent{   
    
    public AbstractManagementEvent(String topic) {
        this.topic = topic;
    }

    @Override
    public Source source() {
        return Source.MANAGEMENT;
    }
    
    @Override
    public String topic() {
        return topic;
    } 
  
    protected String topic;    
}
