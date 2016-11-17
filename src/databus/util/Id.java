package databus.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Qu Chunhe on 2016-11-17.
 */
public class Id {

    public Id() {
        this(0, 22);
    }

    public Id(int initialValue, int bits) {
        autoIncrementId = new AtomicInteger(initialValue);
        BITS = (bits>31) || (bits<1) ? 22 : bits;
        ID_BOUND = (1 << BITS) - 1;
        SERVICE_ID_MASK = (1 << (32 - BITS)) - 1;
    }

    public long next(long unixTime, int serviceId) {
        return (unixTime << 32) | ((serviceId & SERVICE_ID_MASK) << BITS) | next();
    }

    private int next() {
        int currentId;
        do {
            currentId = autoIncrementId.getAndIncrement();
            if (currentId > ID_BOUND) {
                synchronized (lock) {
                    if (autoIncrementId.get() > ID_BOUND) {
                        autoIncrementId.set(0);
                    }
                }
            }
        } while (currentId > ID_BOUND);

        return currentId;
    }

    private final int BITS;
    private final int ID_BOUND;
    private final int SERVICE_ID_MASK;

    private final AtomicInteger autoIncrementId;
    private final Object lock = new Object();
}
