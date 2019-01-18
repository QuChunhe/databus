package databus.util;

import java.util.concurrent.Future;

/**
 * Created by Qu Chunhe on 2018-05-28.
 */
public class FutureRecorder<V> {

    public FutureRecorder(Future<V> future, Callback<V> callback) {
        this.future = future;
        this.callback = callback;
        startTime = System.currentTimeMillis();
    }

    public Future<V> getFuture() {
        return future;
    }

    public Callback<V> getCallback() {
        return callback;
    }

    public long getStartTime() {
        return startTime;
    }

    private final Future<V> future;
    private final Callback<V> callback;
    private final long startTime;
}
