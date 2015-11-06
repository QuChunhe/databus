package databus.event.management;

import databus.event.AbstractEvent;
import databus.event.ManagementEvent;

public abstract class AbstractMgtEvent extends AbstractEvent
                                       implements ManagementEvent{
    
    public AbstractMgtEvent() {
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
    
    public AbstractMgtEvent topic(String topic) {
        this.topic = topic;
        return this;
    }
  
    private String topic;    
}
