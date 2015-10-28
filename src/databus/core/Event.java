package databus.core;

import databus.util.InternetAddress;

public interface Event {
    
    public static enum Source {REDIS, MYSQL, MANAGEMENT}

    public Source source();
    
    public String topic();
    
    public long time();
    
    public Event time(long time);
    
    public String type();
    
    public InternetAddress address();
    
    public Event address(InternetAddress localAddress);
    
}
