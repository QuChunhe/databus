package databus.event;

import databus.core.Event;
import databus.util.InternetAddress;

public abstract class AbstractEvent implements Event{

    @Override
    public long time() {
        return time;
    }

    @Override
    public InternetAddress address() {
        return address;
    }

    @Override
    public void address(InternetAddress localAddress) {
        address = localAddress;
        
    }
    
    protected InternetAddress address;
    protected long time;
}
