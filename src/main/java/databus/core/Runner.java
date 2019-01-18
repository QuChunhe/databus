package databus.core;

public interface Runner {
    
    void initialize();
    
    void runOnce() throws Exception;
    
    void processException(Exception e);
    
    void processFinally();
    
    void stop(Thread owner);
    
    void close();

}
