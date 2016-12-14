package databus.core;

public interface EventFilter extends Initializable {
    
    Event process(Event event);
}
