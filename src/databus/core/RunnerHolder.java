package databus.core;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RunnerHolder implements Startable, Stoppable, Joinable {
    
    public RunnerHolder(Runner runner, String name) {
        setRunner(runner, name);
    }

    public RunnerHolder(Runner runner) {
        this(runner, runner.getClass().getName());
    }

    public RunnerHolder() {
    }

    @Override
    public void join() throws InterruptedException {
        thread.join();
    }

    @Override
    public void stop() {
        if (!isRunning.get()) {
            return;
        }
        isRunning.set(false);

        log.info(thread.getName() + " will stop");
        thread.getRunner().stop(thread);
        if (thread.isAlive()) {
            log.info(thread.getName() + " has not stopped!");
        } else {
            log.info(thread.getName() + " has finished!");
        }
    }

    public void setRunner(Runner runner) {
        setRunner(runner, runner.getClass().getName());
    }

    public void setRunner(Runner runner, String name) {
        thread = new RunnerThread(runner, name);
    }

    public boolean isRunning() {
        return isRunning.get();
    }

    @Override
    public void start() {
        if (isRunning.get()) {
            log.error(thread.getName()+" is running!");
            return;
        }
        isRunning.set(true);
        thread.start();
    }
    
    private final static Log log = LogFactory.getLog(RunnerHolder.class);
    
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private RunnerThread thread;

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
            while (isRunning.get()) {
                try {
                    runner.runOnce();
                } catch (Exception e) {
                    runner.processException(e);
                } finally {
                    runner.processFinally();
                }
            }
            log.info(getName()+" will close");
            runner.close();
        }

        private Runner runner;
    }

}
