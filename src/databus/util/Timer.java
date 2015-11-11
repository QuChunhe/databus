package databus.util;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Timer {    
    
    public Timer(String name) {
        this.name = name;
    }

    public ScheduledFuture<?>  schedule(Runnable task, long delay, TimeUnit unit) {
        return executor.schedule(new Task(task), delay, unit);
    }
    
    public ScheduledFuture<?>  schedulePeriodically(Runnable task, long initialDelay, 
                                                    long period, TimeUnit unit) {
        return executor.scheduleAtFixedRate(new Task(task), initialDelay, period, unit);
    }
    
    public void cancel() {
        for(Runnable task : executor.getQueue()) {
            if (name.equals(((Task)task).name())) {
                executor.remove(task);
            }
        }
    }
    
    public boolean remove(Runnable task) {
        return executor.remove(task);
    }
    
    public boolean contains(Runnable task) {
        return executor.getQueue().contains(task);
    }
    
    private static ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
    
    private String name;
    
    private class Task implements Runnable {
        
        public Task(Runnable task) {
            this.task = task;
        }

        @Override
        public void run() {
            task.run();            
        }
        
        public String name() {
            return name;
        }
        
        private Runnable task;
    }
}
