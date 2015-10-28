package databus.core;

public interface Listener  {
    public void start();
    
    public boolean isRunning();
    
    public void stop();
}
