package databus.event;

import databus.core.Event;
import databus.util.InternetAddress;

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
    public InternetAddress address() {
        return address;
    }

    @Override
    public Event address(InternetAddress localAddress) {
        address = localAddress;
        return this;
    }    

    private InternetAddress address;
    private long time;
}
