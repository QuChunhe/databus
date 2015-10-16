package databus.core;

public interface Subscriber {
    
    public boolean subscribe(String ipAddress, int port, String topic);
    
    public boolean receive(Event event);

}
