package databus.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by Qu Chunhe on 2018-05-30.
 */
public class CallerWaitsPolicy implements RejectedExecutionHandler {
    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        for (;;) {
            try {
                executor.getQueue().put(r);
                return;
            } catch (InterruptedException e) {
                log.error("Has been interrupted", e);
            }
        }
    }

    private final static Log log = LogFactory.getLog(CallerWaitsPolicy.class);
}