package databus.core;

import databus.util.InternetAddress;

public interface Publisher {
    
    public void publish(Event event);
    
    public void publish(InternetAddress remoteAddress, Event event);

}
