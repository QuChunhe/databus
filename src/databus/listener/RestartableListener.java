package databus.listener;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

import databus.core.Restartable;
import databus.network.Publisher;


public abstract class RestartableListener extends AbstractListener implements Restartable {

    public RestartableListener(Publisher publisher) {
        setPublisher(publisher);     
    }
    
    public RestartableListener() {
        this(null);        
    }

    @Override
    public void start() {
        listeners.addLast(this);
        if ((null==monitor) || (monitor.getState()==Thread.State.TERMINATED)) {
            synchronized (lock) {
                if ((null==monitor) || (monitor.getState()==Thread.State.TERMINATED)) {
                    monitor = new Thread(new RunningMonitor(), "Running Monitor");
                    monitor.start();
                }
            }
        }
    }

    @Override
    public void stop() {
        listeners.remove(this);
        if (listeners.isEmpty() && (monitor.getState()!=Thread.State.TERMINATED)) {
            synchronized (lock) {
                if (listeners.isEmpty() && (monitor.getState()!=Thread.State.TERMINATED)) {
                    monitor.interrupt();
                }
            }
        }
    }
    
    final private static long TEN_SECONDS = 10000L;
    
    private static Deque<Restartable> listeners = new ConcurrentLinkedDeque<Restartable>();
    private static Thread monitor = null;
    private static Object lock = new Object();
    
    private static class RunningMonitor implements Runnable {

        @Override
        public void run() {
            while(!listeners.isEmpty())  {
                try {
                    Thread.sleep(TEN_SECONDS);
                } catch (InterruptedException e) {
                    
                }
                for(Restartable listener : listeners) {
                    if (!listener.isRunning()) {
                        listener.restart();
                    }
                }
            }
            Thread.currentThread().interrupt();
        }
        
    }

}
