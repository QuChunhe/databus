package databus.core;

import java.net.InetAddress;

public interface Event{
    
    public static enum Source {REDIS, MYSQL}

    public Source source();
    
    public String topic();
    
    public Event topic(String topic);
    
    public String fullTopic();
    
    public long time();
    
    public Event time(long time);
    
    public String type();
    
    public InetAddress ipAddress();
    
    public Event ipAddress(InetAddress ipAddress);
    
}
