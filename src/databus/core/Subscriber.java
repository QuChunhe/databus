package databus.core;

public interface Subscriber extends Service {
    
    void register(String topic, Receiver receiver);

    void receive(String topic, Event event);
}
