package databus.core;

import java.net.InetAddress;

public interface Event{
    
    enum Source {REDIS, MYSQL}

    Source source();
    
    String topic();
    
    Event topic(String topic);
    
    String fullTopic();
    
    long time();
    
    Event time(long time);
    
    String type();
    
    InetAddress ipAddress();
    
    Event ipAddress(InetAddress ipAddress);
    
}
