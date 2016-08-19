package databus.receiver.redis;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.core.Receiver;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public abstract class RedisReceiver implements Receiver, Closeable {
    
    public RedisReceiver() {
    }

    @Override
    public void initialize(Properties properties) {
        String host = properties.getProperty("redis.host", "127.0.0.1");
        int port = Integer.parseInt(properties.getProperty("redis.port", "6379"));
        int timeout = Integer.parseInt(properties.getProperty("redis.timeout","60"));
        String password = properties.getProperty("redis.password");
        int database = Integer.parseInt(properties.getProperty("redis.database","0"));
        int maxTotal = Integer.parseInt(properties.getProperty("redis.maxTotal","5"));
        int maxIdle = Integer.parseInt(properties.getProperty("redis.maxIdle","3"));
        int minIdle = Integer.parseInt(properties.getProperty("redis.minIdle","0"));
        
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(maxTotal);
        config.setMaxIdle(maxIdle);
        config.setMinIdle(minIdle);
        
        jedisPool = new JedisPool(config, host, port, timeout, password, database);        
    }    

    @Override
    public void receive(Event event) {
        try (Jedis jedis = jedisPool.getResource();) {
            receive(jedis, event);
        } catch(Exception e) {
            log.error("Redis can't save "+event.toString(), e);
        }
    }
    
    @Override
    public void close() throws IOException {
        jedisPool.close();        
    }

    abstract protected void receive(Jedis jedis, Event event);
    
    private static Log log = LogFactory.getLog(RedisReceiver.class);
    
    private JedisPool jedisPool;
}
