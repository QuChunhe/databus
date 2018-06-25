package databus.util;

import java.util.concurrent.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.AbstractService;
import databus.core.Runner;

/**
 * Created by Qu Chunhe on 2018-05-28.
 */
public class FutureChecker extends AbstractService {

    public FutureChecker(int capacity) {
        futureRecorderQueue = new LinkedBlockingQueue<>(capacity);
        setRunner(new CheckingRunner(), "CheckingRunner");
    }

    public FutureChecker() {
        this(2000);
    }

    public <V> void check(Future<V> future, Callback<V> callback){
        try {
            if (futureRecorderQueue.size() == 0) {
                synchronized (lock) {
                    if (futureRecorderQueue.size() == 0) {
                        futureRecorderQueue.put(new FutureRecorder<V>(future, callback));
                        lock.notify();
                    } else {
                        futureRecorderQueue.put(new FutureRecorder<V>(future, callback));
                    }
                }
            } else {
                futureRecorderQueue.put(new FutureRecorder<V>(future, callback));
            }
        } catch (InterruptedException e) {
            log.error("Can not put FutureRecorder!", e);
        }
    }

    public void setTimeoutMilliSeconds(long timeoutMilliSeconds) {
        this.timeoutMilliSeconds = timeoutMilliSeconds;
    }

    public void setExecutor(ThreadPoolExecutor executor) {
        this.executor = executor;
    }

    public void setCorePoolSize(int corePoolSize) {
        this.corePoolSize = corePoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public void setKeepAlivedSeconds(long keepAlivedSeconds) {
        this.keepAlivedSeconds = keepAlivedSeconds;
    }

    public void setCheckIntervalMilliSeconds(long checkIntervalMilliSeconds) {
        this.checkIntervalMilliSeconds = checkIntervalMilliSeconds;
    }

    private final static Log log = LogFactory.getLog(FutureChecker.class);

    private final BlockingQueue<FutureRecorder> futureRecorderQueue;
    private final Object lock = new Object();

    private long timeoutMilliSeconds = 60 * 1000;
    private ThreadPoolExecutor executor;
    private int corePoolSize;
    private int maxPoolSize;
    private long keepAlivedSeconds = 120;
    private long checkIntervalMilliSeconds = 20;


    private class CheckingRunner implements Runner {

        @Override
        public void initialize() {
            if (corePoolSize > 0) {
                if (0 == maxPoolSize) {
                    maxPoolSize = 2 * corePoolSize;
                }
                executor = new ThreadPoolExecutor(corePoolSize,
                                                  maxPoolSize,
                                                  keepAlivedSeconds,
                                                  TimeUnit.SECONDS,
                                                  new ArrayBlockingQueue<>(200),
                                                  new ThreadPoolExecutor.CallerRunsPolicy());
            }
        }

        @Override
        public void runOnce() throws Exception {
            if (futureRecorderQueue.size() == 0) {
                synchronized (lock) {
                    if (futureRecorderQueue.size() == 0) {
                        lock.wait(2000);
                    }
                }
            }
            long startTime = System.currentTimeMillis();
            while (futureRecorderQueue.size() > 0) {
                for(FutureRecorder recorder : futureRecorderQueue) {
                    if (recorder.getFuture().isDone()) {
                        futureRecorderQueue.remove(recorder);
                        executeCallback(recorder.getFuture(), recorder.getCallback());
                    } else {
                        long duration = System.currentTimeMillis() - recorder.getStartTime();
                        if (duration > timeoutMilliSeconds) {
                            futureRecorderQueue.remove(recorder);
                            log.error("Has waited "+duration+" ms for "+recorder.getFuture().toString());
                            Callback callback = recorder.getCallback();
                            if (null != callback) {
                                onFailure(callback, new TimeoutException("Has waited "+duration+" ms"));
                            }
                        }
                    }
                }
                long interval = System.currentTimeMillis() - startTime;
                if (interval < checkIntervalMilliSeconds) {
                    try {
                        Thread.sleep(checkIntervalMilliSeconds-interval);
                    } catch (InterruptedException e) {
                        log.error("Interrupt sleep", e);
                    }
                }
            }
        }

        @Override
        public void processException(Exception e) {
            log.error("Has been interrupted", e);
        }

        @Override
        public void processFinally() {
        }

        @Override
        public void stop(Thread owner) {
        }

        @Override
        public void close() {
        }

        private <V> void executeCallback(Future<V> future, Callback<V> callback) {
            if (null == callback) {
                return;
            }
            try {
                onSuccess(callback, future.get());
            } catch (ExecutionException e) {
                log.error("Has meet execution exception", e);
                onFailure(callback, e.getCause());
            } catch (InterruptedException e) {
                log.error("Has been interrupted", e);
                onFailure(callback, e);
            } catch (Exception e) {
                log.error("Meet exception", e);
            }
        }

        private <V> void onSuccess(Callback<V> callback, V result) {
            if (null == executor) {
                callback.onSuccess(result);
            } else {
                executor.execute(new Runnable() {
                                     @Override
                                     public void run() {
                                         callback.onSuccess(result);
                                     }
                                 });
            }
        }

        private <V> void onFailure(Callback<V> callback, Throwable t) {
            if (null == executor) {
                callback.onFailure(t);
            } else {
                executor.execute(new Runnable() {
                                     @Override
                                     public void run() {
                                         callback.onFailure(t);
                                     }
                                 });
            }
        }
    }
}
