package databus.core;

public interface Receiver extends Initializable {    
    
    public void receive(Event event);

}
