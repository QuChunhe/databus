package databus.core;

public interface Startable {
    
    public void start();
    
    public boolean isRunning();
    
    public void stop();

}
