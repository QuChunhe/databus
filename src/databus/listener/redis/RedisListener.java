package databus.listener.redis;

import java.util.Properties;

import databus.event.RedisEvent;
import databus.listener.AbstractListener;
import redis.clients.jedis.Jedis;

public abstract class RedisListener extends AbstractListener {

    public RedisListener(String name) {
        super(name);
    }
    
    public RedisListener() {
        this("RedisListener");
    }

    @Override
    public void initialize(Properties properties) {
        host = properties.getProperty("redis.host", "127.0.0.1");
        port = Integer.parseInt(properties.getProperty("redis.port", "6379"));
        timeout = Integer.parseInt(properties.getProperty("redis.timeout", "60"));

        jedis = new Jedis(host, port, timeout);
    }

    @Override
    protected void runOnce(boolean hasException) throws Exception {
        if (hasException) {
            jedis = new Jedis(host, port, timeout);
        }
        RedisEvent event = listen();
        if (null != event) {
           publisher.publish(event); 
        }        
    }

    protected abstract RedisEvent listen();

    protected Jedis jedis;
    
    private String host;
    private int port;
    private int timeout;
}
