package databus.core;

import databus.util.InternetAddress;

public interface Publisher {
    
    public void publish(Event event);
    
    public void publish(InternetAddress remoteAddress, Event event);
    
    public void subscribe(String topic, InternetAddress remoteAddress);

    void unsubscribe(String topic, InternetAddress remoteAddress);
    
    

}
