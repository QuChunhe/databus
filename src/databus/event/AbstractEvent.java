package databus.event;

import databus.core.Event;

public abstract class AbstractEvent implements Event{

    @Override
    public long time() {
        return time;
    }
    
    @Override
    public Event time(long time) {
        this.time = time;
        return this;
    }
    
    @Override
    public String ipAddress() {
        return this.ipAddress;
    }

    @Override
    public Event ipAddress(String ipAddress) {
        this.ipAddress = ipAddress;
        return this;
    }
    
    private String ipAddress;
    private long time;
}
