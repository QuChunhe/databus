package databus.util;

import java.io.Closeable;

/**
 * Created by Qu Chunhe on 2019-09-27.
 */
public interface ObjectPool<T> extends Closeable {

    void returnBrokenResource(final T resource) throws Exception;

    void returnResource(final T resource);

    T getResource();
}
