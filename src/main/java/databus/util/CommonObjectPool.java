package databus.util;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * Created by Qu Chunhe on 2019-09-27.
 */
public class CommonObjectPool<T> implements ObjectPool<T> {
    public CommonObjectPool(final PooledObjectFactory<T> factory,
                            final GenericObjectPoolConfig<T> config) {
        internalPool = new GenericObjectPool<T>(factory, config);
    }

    @Override
    public void returnBrokenResource(final T resource) throws Exception {
        if (resource == null) {
            return;
        }
        internalPool.invalidateObject(resource);
    }

    @Override
    public void returnResource(T resource) {
        if (resource == null) {
            return;
        }
        internalPool.returnObject(resource);
    }

    @Override
    public T getResource() {
        try {
            return internalPool.borrowObject();
        } catch (Exception e) {
            log.error("Can not get resource", e);
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        internalPool.isClosed();
    }

    protected final GenericObjectPool<T> internalPool;

    private final static Log log = LogFactory.getLog(CommonObjectPool.class);
}
