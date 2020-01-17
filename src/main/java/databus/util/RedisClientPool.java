package databus.util;

import java.io.Closeable;

/**
 * Created by Qu Chunhe on 2019-09-27.
 */
public interface RedisClientPool extends Closeable {

    RedisClient getRedisClient();
}