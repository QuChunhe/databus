package databus.core;

public interface Subscriber extends Initializable, Joinable, Startable, Stoppable {
    
    public void register(String topic, Receiver receiver);
}
