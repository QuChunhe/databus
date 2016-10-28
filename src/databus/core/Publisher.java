package databus.core;

public interface Publisher extends Initializable, Stoppable, Joinable {
    
    void publish(Event event);

    void addListener(Listener listener);

}
