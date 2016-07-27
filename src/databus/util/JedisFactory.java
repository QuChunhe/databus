package databus.util;

import java.util.Properties;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisFactory {
    
    public static JedisPool creatPool(Properties properties) {
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
        
        return new JedisPool(config, host, port, timeout, password, database);
    }
}
