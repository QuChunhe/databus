package databus.util;

import java.math.BigInteger;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Qu Chunhe on 2016-11-17.
 */
public class Id {

    public Id() {
        this(22);
    }

    public Id(int bits) {
        BITS = (bits>31) || (bits<1) ? 22 : bits;
        ID_BOUND = (1 << BITS) - 1;
        SERVICE_ID_MASK = (1 << (32 - BITS)) - 1;
        autoIncrementId = new AtomicInteger(ThreadLocalRandom.current().nextInt(ID_BOUND));
    }

    public BigInteger next(long unixTime, int serviceId) {
        BigInteger highPart = new BigInteger(Long.toUnsignedString(unixTime));
        highPart = highPart.shiftLeft(32);
        long lowPart = ((serviceId & SERVICE_ID_MASK) << BITS) | next();

        return highPart.or(new BigInteger(Long.toUnsignedString(lowPart)));
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
    private final long SERVICE_ID_MASK;

    private final AtomicInteger autoIncrementId;
    private final Object lock = new Object();
}
