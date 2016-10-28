package databus.core;

public interface Restartable {
    
    boolean isRunning();
    
    void restart();

}
