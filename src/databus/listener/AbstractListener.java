package databus.listener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Listener;
import databus.network.Publisher;

public abstract class AbstractListener implements Listener, Runnable{
    
    public AbstractListener(Publisher publisher) {
        this.publisher = publisher;
        runner = new Thread(this);
        doesRun = false;
    }
    
    public AbstractListener() {
        this(null);
    }
    
    public void setPublisher(Publisher publisher) {
        this.publisher = publisher;
    }
    
    @Override
    public void start() {
        if (!doesRun) {
            doesRun = true;
            runner.start();
        }
    }

    @Override
    public boolean isRunning() {
        return doesRun;
    }

    @Override
    public void stop() {
        if (doesRun) {
           doesRun = false;
           runner.interrupt(); 
        }        
    }
    
    @Override
    public void run() {
        int exceptionCount = 0;
        while (doesRun) {
            try {
                runOnce(exceptionCount > 0);
                exceptionCount = 0;
            } catch (Exception e) {
                exceptionCount++;
                log.error("Some exceptions happen", e);
                if (exceptionCount <= 10) {
                    //try again immediately
                } else if  (exceptionCount <= 600) {// 10 Minutes
                    sleep(1);
                } else if (exceptionCount < 960){ // 1 hour
                    sleep(10);
                } else if (exceptionCount < 2400){ // 1 day
                    sleep(60);
                } else {
                    //avoid overflow
                    exceptionCount = 960;
                }
            }
        }
    }    
    
    private void sleep(long duration) {
        try {
            Thread.sleep(duration * 1000);
        } catch(InterruptedException e) {
            log.warn("Sleep has been interrupted",e);
        }        
    }
    
    abstract protected void runOnce(boolean hasException) throws Exception;    

    protected Publisher publisher;
    
    private static Log log = LogFactory.getLog(AbstractListener.class);
    
    private Thread runner;
    //only one thread modify doesRun
    private volatile boolean doesRun;
}
