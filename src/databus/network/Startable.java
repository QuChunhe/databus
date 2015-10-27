package databus.network;

public interface Startable {
    
    public void start();
    
    public boolean isRunning();
    
    public void stop();

}
