package databus.core;

public interface Publisher extends Initializable, Stoppable, Joinable {
    
    public void publish(Event event);

    public void addListener(Listener listener);
}
