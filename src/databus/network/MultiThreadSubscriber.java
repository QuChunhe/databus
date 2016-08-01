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

        for(Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                log.warn(t.getName()+" is interrupted", e);
            }
            if (!doesRun) {
                break;
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
            initializeOnce();
            threads = new Thread[threadNumber];
            doesRun = true;
            for (int i=0; i<threadNumber; i++) {
                threads[i] = new Thread(new Runnable() {
                                    @Override
                                    public void run() {
                                        initializePerThread();
                                        while (doesRun) {
                                            run0();
                                        }
                                        destroyPerThread();
                                    }                
                                 }, name+"-"+i);
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
    
    protected abstract void initializeOnce();
    
    protected abstract void initializePerThread();
    
    protected abstract void destroyPerThread();
    
    private static Log log = LogFactory.getLog(MultiThreadSubscriber.class);
    
    private Thread[] threads = null;
    private final int threadNumber;
    private final String name;
    protected volatile boolean doesRun;
}
