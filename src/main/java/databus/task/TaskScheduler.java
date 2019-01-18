package databus.task;

import java.io.Closeable;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by Qu Chunhe on 2017-03-30.
 */
public class TaskScheduler implements Closeable {
    public TaskScheduler(int corePoolSize) {
        executor = new ScheduledThreadPoolExecutor(corePoolSize);
    }

    public TaskScheduler() {
        this(1);
    }

    public void setMaximumPoolSize(int maximumPoolSize) {
        executor.setMaximumPoolSize(maximumPoolSize);
    }

    public void setKeepAliveTime(long time, TimeUnit unit) {
        executor.setKeepAliveTime(time, unit);
    }

    public void setKeepAliveSeconds(long keepAliveSeconds) {
        setKeepAliveTime(keepAliveSeconds, TimeUnit.SECONDS);
    }

    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return executor.schedule(command, delay, unit);
    }

    public ScheduledFuture<?> schedule(Runnable command, ZonedDateTime startTime, Period period) {
        if (null == period) {
            return schedule(command, startTime);
        }
        long periodSeconds = period.toSeconds();
        long delaySeconds = ZonedDateTime.now().until(startTime, ChronoUnit.SECONDS);
        while (delaySeconds <= 0) {
            delaySeconds += periodSeconds;
        }
        return executor.scheduleAtFixedRate(command, delaySeconds, periodSeconds, TimeUnit.SECONDS);
    }

    public ScheduledFuture<?> schedule(Runnable command, ZonedDateTime startTime) {
        long delaySeconds = ZonedDateTime.now().until(startTime, ChronoUnit.SECONDS);
        if (delaySeconds < 0) {
            log.error(startTime.toString()+" has passed!");
            return null;
        }
        return schedule(command, delaySeconds, TimeUnit.SECONDS);
    }

    public void addTask(ScheduledTask task) {
        schedule(task, task.getStartTime(), task.getPeriod());
    }

    public void setTasks(Collection<ScheduledTask> tasks) {
        for(ScheduledTask t : tasks) {
            addTask(t);
        }
    }

    public void setWaitingSecondesWhenClose(long waitingSecondesWhenClose) {
        this.waitingSecondesWhenClose = waitingSecondesWhenClose;
    }

    @Override
    public void close() throws IOException {
        executor.shutdown();
        try {
            executor.awaitTermination(waitingSecondesWhenClose, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted when waiting termination!", e);
        }
    }

    public void shutdown() {

    }

    List<Runnable> shutdownNow() {
        return executor.shutdownNow();
    }

    private final static Log log = LogFactory.getLog(TaskScheduler.class);

    private final ScheduledThreadPoolExecutor executor;

    private long waitingSecondesWhenClose = 3;
}
