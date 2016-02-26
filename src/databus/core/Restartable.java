package databus.core;

public interface Restartable {
    
    public boolean isRunning();
    
    public void restart();

}
