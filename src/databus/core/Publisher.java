package databus.core;

public interface Publisher extends Initializable, Stoppable {
    
    public void publish(Event event);
}
