package databus.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Qu Chunhe on 2019-07-22.
 */
public class ExecutingCounter {

    public void setThreshold(int threshold) {
        this.threshold = threshold;
    }

    public void beforeExecuting() {
        if ((waitingCounter>0) || (executingCounter.get()>(threshold-1))) {
            synchronized (lock) {
                waitingCounter++;
                try {
                    while (executingCounter.get() > (threshold-1)) {
                        lock.wait(ThreadLocalRandom.current().nextInt(1000));
                    }
                } catch (InterruptedException e) {
                    log.error("Wake up!", e);
                } finally {
                    waitingCounter--;
                }
                if (executingCounter.incrementAndGet() > threshold) {
                    log.error("counter error: "+ executingCounter.get());
                }
            }
        }
    }

    public void afterExecuting() {
        executingCounter.decrementAndGet();
        if (waitingCounter>0) {
            synchronized (lock) {
                if (waitingCounter > 0) {
                    lock.notify();
                }
            }
        }
    }

    private final static Log log = LogFactory.getLog(ExecutingCounter.class);

    private int threshold = 500;
    private int waitingCounter = 0;

    private final AtomicInteger executingCounter = new AtomicInteger(0);
    private final Object lock = new Object();


}
