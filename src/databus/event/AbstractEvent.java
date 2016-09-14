package databus.event;

import java.net.InetAddress;

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
    public InetAddress ipAddress() {
        return ipAddress;
    }

    @Override
    public Event ipAddress(InetAddress ipAddress) {
        this.ipAddress = ipAddress;
        return this;
    }
    
    public String fullTopic() {
        return null==ipAddress ? null : ipAddress.getHostAddress() + topic();
    }
    
    public Event topic(String topic) {
        this.topic = topic;
        return this;
    }
    
    public String topic() {
        return null==topic ? defaultTopic() : topic;
    }
    
    protected abstract String defaultTopic();

    private InetAddress ipAddress;
    private long time;
    private String topic = null;;
}
