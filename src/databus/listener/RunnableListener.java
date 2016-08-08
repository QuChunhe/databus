package databus.listener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Publisher;

public abstract class RunnableListener extends AbstractListener implements Runnable {    
        
    public RunnableListener(Publisher publisher, String name) {
        setPublisher(publisher);
        runner = new Thread(this, name);
        doesRun = false;
    }
    
    public RunnableListener(String name) {
        this(null, name);
    }
    
    public RunnableListener() {
        this(RunnableListener.class.getSimpleName());
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
            log.info("Waiting " + this.getClass().getName());
            try {
                runner.join();
            } catch (InterruptedException e) {

            }
        }       
    }   
   
    @Override
    public void run() {
        int exceptionCount = 0;
        while (doesRun) {
            try {
                runOnce(exceptionCount > 0);
                exceptionCount = 0;
            } catch(InterruptedException e) {
                log.error(runner.getName() + " is interrupted!", e);
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
        close();
    } 
    
    abstract protected void runOnce(boolean hasException) throws Exception;
    
    abstract protected void close();
    
    private void sleep(long duration) {
        try {
            Thread.sleep(duration * 1000);
        } catch(InterruptedException e) {
            log.warn("Sleep has been interrupted",e);
        }        
    }
    
    private static Log log = LogFactory.getLog(RunnableListener.class);
    
    private Thread runner;
    //only one thread modify doesRun
    private volatile boolean doesRun;
}
