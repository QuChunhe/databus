package databus.receiver;

import java.util.Properties;

import databus.core.Receiver;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public abstract class RedisReceiver implements Receiver {
    
    public RedisReceiver() {

    }

    @Override
    public void initialize(Properties properties) {
        String host = properties.getProperty("host", "127.0.0.1");
        int port = Integer.parseInt(properties.getProperty("port", "6379"));
        int timeout = Integer.parseInt(properties.getProperty("timeout","60"));
        String password = properties.getProperty("password");
        int database = Integer.parseInt(properties.getProperty("database","0"));
        int maxTotal = Integer.parseInt(properties.getProperty("maxTotal","5"));
        int maxIdle = Integer.parseInt(properties.getProperty("maxIdle","3"));
        int minIdle = Integer.parseInt(properties.getProperty("minIdle","0"));
        
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(maxTotal);
        config.setMaxIdle(maxIdle);
        config.setMinIdle(minIdle);
        
        jedisPool = new JedisPool(config, host, port, timeout, password, database);        
    }

    protected Jedis getJedis() {
        return jedisPool.getResource();
    }
    
    private JedisPool jedisPool;
}
