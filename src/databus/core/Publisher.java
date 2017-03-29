package databus.core;

public interface Publisher extends Endpoint {
    
    void publish(Event event);

    void addListener(Listener listener);

}
