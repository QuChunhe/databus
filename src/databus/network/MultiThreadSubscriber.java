package databus.network;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public abstract class MultiThreadSubscriber extends AbstractSubscriber {    

    public MultiThreadSubscriber() {
        this(1);
    }
    
    public MultiThreadSubscriber(int threadNumber) {
       this(threadNumber, "MultiThreadSubscriber");          
    }
    
    public MultiThreadSubscriber(int threadNumber, String name) {
        super();
        if (threadNumber < 1) {
            throw new IllegalArgumentException(threadNumber + " thread is illegal");
        }
        this.threadNumber = threadNumber;
        this.name = name;            
    }

    @Override
    public void join() throws InterruptedException {
        if (null == threads) {
            log.warn("Has't thread");
            return;
        }
        long ONE_SECOND = 1000;
        for(Thread t : threads) {
            try {
                if (t.isAlive()) {
                    t.join();
                } else if (doesRun){
                    log.error(t.getName()+" is "+t.getState());
                    Thread.sleep(ONE_SECOND);
                }                
            } catch (InterruptedException e) {
                log.warn(t.getName()+" is interrupted", e);
            }
        }                
    }

    @Override
    public boolean isRunning() {
        if (null==threads) {
            return false;
        }
        for(Thread t : threads) {
            if (t.getState() != Thread.State.TERMINATED) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void start() {
        if (null == threads) { 
            initialize();
            threads = new Thread[threadNumber];
            doesRun = true;
            for (int i=0; i<threadNumber; i++) {
                threads[i] = new Thread(createWorker(), name+"-"+i);
                threads[i].start();
            }
        }         
    }

    @Override
    public void stop() {
        if (null == threads) {
            return;
        }
        doesRun = false;
        for(Thread t : threads) {
            if (t.isAlive()) {
                t.interrupt();
            } 
        }                
    }
    
    protected abstract Worker createWorker();
    
    protected abstract void initialize();
    
    private static Log log = LogFactory.getLog(MultiThreadSubscriber.class);
    
    private Thread[] threads = null;
    private final int threadNumber;
    private final String name;
    protected volatile boolean doesRun;
    
    protected abstract class Worker implements Runnable {
        
        public abstract void initialize();
        
        public abstract void destroy();
        
        public abstract void run0();

        @Override
        public void run() {
            initialize();
            while (doesRun) {
               run0();
            }
            destroy();            
        }   
        
    }
}
