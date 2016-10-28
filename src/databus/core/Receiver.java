package databus.core;

public interface Receiver extends Initializable {    
    
    void receive(Event event);

}
