package databus.core;

public interface Publisher extends Endpoint {
    
    void publish(Event event);

    void publish(String topic, Event event);

    void addListener(Listener listener);

}
