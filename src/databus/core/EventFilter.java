package databus.core;

public interface EventFilter extends Initializable {
    
    public boolean reject(Event event);

}
