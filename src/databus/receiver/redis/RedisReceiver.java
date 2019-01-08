package databus.receiver.redis;

import java.io.IOException;

import databus.util.JedisPoolFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import databus.core.Event;
import databus.core.Receiver;

public abstract class RedisReceiver implements Receiver {
    
    public RedisReceiver() {
        super();
    }

    public void setConfigFile(String configFile) {
        jedisPool = JedisPoolFactory.create(configFile);
    }

    @Override
    public void receive(final Event event) {
        try (Jedis jedis = jedisPool.getResource()) {
            receive(jedis, event);
        } catch(Exception e) {
            log.error("Redis can not save "+event.toString(), e);
        }
    }
    
    @Override
    public void close() throws IOException {
        jedisPool.close();        
    }

    abstract protected void receive(Jedis jedis, Event event);

    private final static Log log = LogFactory.getLog(RedisReceiver.class);
    
    private JedisPool jedisPool;
}
