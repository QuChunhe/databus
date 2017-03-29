package databus.core;

public interface Subscriber extends Endpoint {
    
    void register(String topic, Receiver receiver);

}
