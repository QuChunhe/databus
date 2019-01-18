package databus.task;

import java.time.ZonedDateTime;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by Qu Chunhe on 2017-03-21.
 */
public final class ScheduledTask implements Runnable {
    /**
     * @param startTime format "2017-03-30T12:12:06.488+08:00[Asia/Shanghai]"
     * @param interval
     * @param timeUnit
     */
    public ScheduledTask(String startTime, long interval, String timeUnit) {
        this(ZonedDateTime.parse(startTime),
             new Period(interval, TimeUnit.valueOf(timeUnit.toUpperCase())));
    }

    public ScheduledTask(ZonedDateTime startTime) {
        this(startTime, null);
    }

    public ScheduledTask(ZonedDateTime startTime, Period period) {
        this.startTime = startTime;
        this.period = period;
    }

    /**
     * @param startTime format "2017-03-30T12:12:06.488+08:00[Asia/Shanghai]"
     */
    public ScheduledTask(String startTime) {
        this(ZonedDateTime.parse(startTime));
    }

    public ZonedDateTime getStartTime() {
        return startTime;
    }

    public Period getPeriod() {
        return period;
    }

    public void setCommand(Runnable command) {
        this.command = command;
    }

    @Override
    public void run() {
        try {
            command.run();
        } catch (Exception e) {
            log.error("Command can not runn!", e);
        }
    }

    private final static Log log = LogFactory.getLog(ScheduledTask.class);

    private final ZonedDateTime startTime;
    private final Period period;

    private Runnable command;
}
