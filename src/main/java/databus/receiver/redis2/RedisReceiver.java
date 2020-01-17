package databus.receiver.redis2;

import java.io.IOException;

import databus.core.Event;
import databus.core.Receiver;

import databus.util.RedisClientPool;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import databus.util.RedisClient;

/**
 * Created by Qu Chunhe on 2019-09-24.
 */
public abstract class RedisReceiver implements Receiver {

    public RedisReceiver() {
        super();
    }

    public void setRedisClientPool(RedisClientPool redisClientPool) {
        this.redisClientPool = redisClientPool;
    }

    @Override
    public void receive(final Event event) {
        try (RedisClient redisClient = redisClientPool.getRedisClient()) {
            receive(redisClient, event);
        } catch(Exception e) {
            log.error("Redis can not save "+event.toString(), e);
        }
    }

    abstract public void receive(final RedisClient redisClient, final Event event);

    @Override
    public void close() throws IOException {
        redisClientPool.close();
    }

    private final static Log log = LogFactory.getLog(RedisReceiver.class);

    protected RedisClientPool redisClientPool;
}