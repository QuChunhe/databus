package databus.core;

public interface PublisherClient extends Publisher{
    
    public void subscribe(String topic, Subscriber subscriber);
    
    public void unsubscribe(String topic, Subscriber subscriber);
}
