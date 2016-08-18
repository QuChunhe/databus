package databus.listener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Publisher;
import databus.core.Runner;
import databus.core.ThreadHolder;

public abstract class RunnableListener extends AbstractListener {    
        
    public RunnableListener(Publisher publisher, String name) {
        setPublisher(publisher);
        holder = new ThreadHolder();
    }
    
    public RunnableListener(String name) {
        this(null, name);
    }
    
    public RunnableListener() {
        this(RunnableListener.class.getSimpleName());
    }
    
    @Override
    public void start() {
        if (!isRunning()) {
            holder.add(createListeningRunner());
            holder.start();
        }
    }

    @Override
    public boolean isRunning() {
        return holder.isRunning();
    }

    @Override
    public void stop() {
        holder.stop();      
    }    
    
    @Override
    public void join() throws InterruptedException {
        holder.join();        
    }

    protected abstract ListeningRunner createListeningRunner();

    
    protected static abstract class ListeningRunner implements Runner {        
        
        @Override
        public void runOnce() {
            exceptionCount = 0;            
        }

        @Override
        public void processException(Exception e) {
            exceptionCount++;
            log.error("An exception happens!", e);
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
        
        private void sleep(long duration) {
            try {
                Thread.sleep(duration * 1000);
            } catch (InterruptedException e) {
                log.warn("Sleep has been interrupted", e);
            }
        }
        
        private int exceptionCount = 0;
    }
    
    private static Log log = LogFactory.getLog(RunnableListener.class);
    
    private ThreadHolder holder;    
    
}
