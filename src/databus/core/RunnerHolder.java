package databus.core;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class RunnerHolder implements Startable, Stoppable, Joinable {
    
    public RunnerHolder(Runner... runners) {
        threads = new RunnerThread[runners.length];
        for(int i=0; i<threads.length; i++) {
            threads[i] = new RunnerThread(runners[i], i+"-"+runners[i].getClass().getName());
        }
    }
    
    public RunnerHolder(Collection<Runner> runners) {
        this(runners.toArray(new Runner[runners.size()]));
    }

    @Override
    public void join() throws InterruptedException {
        int count = 0;
        for(RunnerThread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                log.error(t.getClass().getName()+" has finished!", e);
                count++;
            }
        }
        if (count > 0) {
            throw new InterruptedException(count+" runners in total "+threads.length+
                                           " are interrupted!");
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
                log.info(t.getRunner().getClass().getName()+" has not stopped!");
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
    
    private final static Log log = LogFactory.getLog(RunnerHolder.class);
    
    private final AtomicBoolean doesRun = new AtomicBoolean(false);
    private final RunnerThread[] threads;
    
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
