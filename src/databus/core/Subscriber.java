package databus.core;

public interface Subscriber {    
    
    public boolean receive(Event event);

}
