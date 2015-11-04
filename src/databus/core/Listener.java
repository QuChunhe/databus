package databus.core;

public interface Listener extends Initializable {
    public void start();
    
    public boolean isRunning();
    
    public void stop();
}
