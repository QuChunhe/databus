package databus.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Qu Chunhe on 2018-06-23.
 */
public class OperationCounter {
    public OperationCounter() {
        totalCount = new AtomicInteger(0);
        successCount = new AtomicInteger(0);
        failureCount =   new AtomicInteger(0);
    }

    public void addTotalCount(int count) {
        totalCount.addAndGet(count);
    }

    public void addSuccessCount(int count) {
        successCount.addAndGet(count);
        checkCompletion();
    }

    public void addFailureCount(int count) {
        failureCount.addAndGet(count);
        checkCompletion();
    }

    public int getTotalCount() {
        return totalCount.get();
    }

    public int getSuccessCount() {
        return successCount.get();
    }

    public int getFailureCount() {
        return failureCount.get();
    }

    public void waitOnCompletion(long timeoutMSec) throws InterruptedException {
        if ((getSuccessCount()+getFailureCount()) >= getTotalCount()) {
            return;
        }
        synchronized (lock) {
            if ((getSuccessCount()+getFailureCount()) >= getTotalCount()) {
                return;
            } else {
                wait(timeoutMSec);
            }
        }
    }

    private void checkCompletion() {
        if ((getSuccessCount()+getFailureCount()) >= getTotalCount()) {
            synchronized (lock) {
                if ((getSuccessCount()+getFailureCount()) >= getTotalCount()) {
                    lock.notify();
                }
            }
        }
    }

    private final AtomicInteger totalCount;
    private final AtomicInteger successCount;
    private final AtomicInteger failureCount;

    private final Object lock = new Object();
}
