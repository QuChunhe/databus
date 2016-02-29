package databus.core;

import java.net.InetAddress;

public interface Event{
    
    public static enum Source {REDIS, MYSQL, MANAGEMENT, CONFIRMATION}

    public Source source();
    
    public String topic();
    
    public long time();
    
    public Event time(long time);
    
    public String type();
    
    public InetAddress ipAddress();
    
    public Event ipAddress(InetAddress ipAddress);
    
}
