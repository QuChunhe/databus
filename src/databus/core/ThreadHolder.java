package databus.core;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class ThreadHolder implements Startable, Stoppable, Joinable {    
    
    public ThreadHolder(Runner... runners) {
        threads = new RunnerThread[runners.length];
        for(int i=0; i<threads.length; i++) {
            threads[i] = new RunnerThread(runners[i], runners[i].getClass().getName()+"-"+i);
        }
    }
    
    public ThreadHolder(Collection<Runner> runners) {
        this(runners.toArray(new Runner[runners.size()]));
    }

    @Override
    public void join() throws InterruptedException {
        StringBuilder builder = new StringBuilder();
        for(RunnerThread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                String message = e.getMessage();
                if (builder.length() > 0) {
                    message += "\n";
                }
                builder.append(message);
            }
        }
        if (builder.length() > 0) {
            throw new InterruptedException(builder.toString());
        }        
    }

    @Override
    public void stop() {
        if (!doesRun.get()) {
            return;
        }
        doesRun.set(false);
        for(RunnerThread t : threads) {
            log.info(t.getRunner().getClass().getName()+" will stop");
            t.getRunner().stop(t);
            if (t.isAlive()) {
                log.info(t.getRunner().getClass().getName()+" hasn't stopped!");                
            } else {
                log.info(t.getRunner().getClass().getName()+" has finished!");
            }
        }
    }

    public boolean isRunning() {
        return doesRun.get();
    }

    @Override
    public void start() {
        if (doesRun.get()) {
            return;
        }
        doesRun.set(true);
        for(RunnerThread t : threads) {
            t.start();
        }
    }
    
    private static Log log = LogFactory.getLog(ThreadHolder.class);
    
    private AtomicBoolean doesRun = new AtomicBoolean(false);
    private RunnerThread[] threads;
    
    private class RunnerThread extends Thread {
        
        public RunnerThread(Runner runner, String name) {
            super(name);
            if (null == runner) {
                throw new NullPointerException("Runner is null!");
            }
            this.runner = runner;
        }
        
        public Runner getRunner() {
            return runner;
        }

        @Override
        public void run() {   
            runner.initialize();
            while (doesRun.get()) {
                try {
                    runner.runOnce();
                } catch (Exception e) {
                    runner.processException(e);
                } finally {
                    runner.processFinally();
                }
            }
            log.info(runner.getClass().getName()+" will close");
            runner.close();
        }
        
        private Runner runner;
    }

}
