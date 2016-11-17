package databus.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Qu Chunhe on 2016-11-17.
 */
public class Id {

    public Id() {
        this(0);
    }

    public Id(int initialValue) {
        id = new AtomicInteger(initialValue);
    }

    public long next(long unixTime, int serviceId) {
        return (unixTime << 32) | ((serviceId & SERVICE_ID_MASK) << 22) | next();
    }

    private int next() {
        int currentId;
        do {
            currentId = id.getAndIncrement();
            if (currentId > ID_BOUND) {
                synchronized (lock) {
                    if (id.get() > ID_BOUND) {
                        id.set(0);
                    }
                }
            }
        } while (currentId > ID_BOUND);

        return currentId;
    }

    private final static int ID_BOUND = (1 << 22) - 1;
    private final static int SERVICE_ID_MASK = (1 << 10) - 1;

    private final AtomicInteger id;
    private final Object lock = new Object();
}
