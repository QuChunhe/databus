package databus.core;

public interface Runner {
    
    public void initialize();
    
    public void runOnce();
    
    public void processException(Exception e);
    
    public void stop(Thread owner);
    
    public void close();
}
