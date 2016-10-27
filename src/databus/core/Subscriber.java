package databus.core;

public interface Subscriber extends Initializable, Joinable, Startable, Stoppable {
    
    void register(String topic, Receiver receiver);
}
