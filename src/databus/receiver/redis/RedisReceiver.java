package databus.receiver.redis;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import databus.core.Event;
import databus.core.Receiver;
import databus.util.Helper;

public abstract class RedisReceiver implements Receiver {
    
    public RedisReceiver() {
        super();
    }

    public void setConfigFile(String configFile) {
        initialize(Helper.loadProperties(configFile));
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

    private void initialize(Properties properties) {
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

    private final static Log log = LogFactory.getLog(RedisReceiver.class);
    
    private JedisPool jedisPool;
}
