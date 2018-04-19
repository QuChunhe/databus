package databus.event;

import databus.core.Event;

public abstract class AbstractEvent implements Event {

    @Override
    public long time() {
        return time;
    }
    
    @Override
    public Event time(long time) {
        this.time = time;
        return this;
    }

    protected abstract String defaultTopic();

    private long time;
}
