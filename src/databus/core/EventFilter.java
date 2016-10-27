package databus.core;

public interface EventFilter extends Initializable {
    
    boolean doesReject(Event event);
}
