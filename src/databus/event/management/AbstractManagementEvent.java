package databus.event.management;

import databus.core.Event;
import databus.event.ManagementEvent;

public abstract class AbstractManagementEvent implements ManagementEvent{   
    
    public AbstractManagementEvent(String topic) {
        this.topic = topic;
    }

    @Override
    public Source source() {
        return Event.Source.MANAGEMENT;
    }
    
    @Override
    public String ipAddress() {
        return ipAddress;
    }
    
    @Override
    public int port() {
        return port;
    }
    
    @Override
    public String topic() {
        return topic;
    } 
    
    @Override
    public long time() {
        return time;
    }  
    
    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }
    public void setPort(int port) {
        this.port = port;
    }

    protected String ipAddress;
    protected int port;
    protected String topic;
    protected long time;
}
