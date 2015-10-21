package databus.core;

public interface PublisherServer extends Publisher{
    
    public void subscribe(String topic, IpAddress ipAddress);
    
    public void unsubscribe(String topic, IpAddress ipAddress);
}
