package databus.network;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class SingleThreadSubscriber extends AbstractSubscriber {    

    public SingleThreadSubscriber() {
        super();
    }

    @Override
    public void join() throws InterruptedException {
        if ((null!=thread) && (thread.isAlive())) {
            thread.join(); 
        }
               
    }

    @Override
    public boolean isRunning() {
        return (null!=thread) && (thread.getState()!=Thread.State.TERMINATED);
    }

    @Override
    public void start() {
        if (null == thread) {
            thread = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                run0();               
                            }                
                         }, this.getClass().getSimpleName());
            thread.start();
        }         
    }

    @Override
    public void stop() {
        if ((null != thread) && (thread.isAlive())) {
            thread.interrupt();
            log.info("Waiting "+this.getClass().getName());
            try {
                thread.join();
            } catch (InterruptedException e) {
                
            }
        } 
    }
    
    protected abstract void run0(); 
    
    private Thread thread = null;
    
    private static Log log = LogFactory.getLog(SingleThreadSubscriber.class);
}
