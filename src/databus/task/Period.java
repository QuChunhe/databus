package databus.task;

import java.util.concurrent.TimeUnit;

/**
 * Created by Qu Chunhe on 2017-03-30.
 */
public final class Period {
    public Period(long interval, TimeUnit timeUnit) {
        this.interval = interval;
        this.timeUnit = timeUnit;
    }

    public long interval() {
        return interval;
    }

    public TimeUnit timeUnit() {
        return timeUnit;
    }

    public long toSeconds() {
        return timeUnit.toSeconds(interval);
    }

    private final long interval;
    private final TimeUnit timeUnit;
}
