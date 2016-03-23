package databus.event.redis;

import databus.core.Event;
import databus.event.AbstractEvent;
import databus.event.RedisEvent;

public abstract class AbstractRedisEvent extends AbstractEvent 
                                         implements RedisEvent {

    public AbstractRedisEvent(String key) {
        super();
        this.key = key;
    }
    
    public AbstractRedisEvent() {
        this(null);
    }

    @Override
    public Source source() {
        return Event.Source.REDIS;
    }

    @Override
    public String topic() {
        return "/"+source()+"/"+type()+"/"+key();
    }

    @Override
    public String key() {
        return key;
    }
    
    public AbstractRedisEvent key(String key) {
        this.key = key;
        return this;
    }
    
    
    
    private String key;

}
