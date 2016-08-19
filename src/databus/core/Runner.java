package databus.core;

public interface Runner {
    
    public void initialize();
    
    public void runOnce() throws Exception;
    
    public void processException(Exception e);
    
    public void processFinally();
    
    public void stop(Thread owner);
    
    public void close();
}
