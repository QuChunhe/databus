package databus.listener.redis;

import java.util.Properties;

import databus.event.RedisEvent;
import databus.listener.AbstractListener;
import redis.clients.jedis.Jedis;

public abstract class RedisListener extends AbstractListener {

    public RedisListener() {
        super();
    }

    @Override
    public void initialize(Properties properties) {
        String host = properties.getProperty("redis.host", "127.0.0.1");
        int port = Integer.parseInt(properties.getProperty("redis.port", "6379"));
        int timeout = Integer.parseInt(properties.getProperty("redis.timeout", "60"));

        jedis = new Jedis(host, port, timeout);
    }

    @Override
    protected void runOnce() throws Exception {
        RedisEvent event = listen();
        if (null != event) {
           publisher.publish(event); 
        }        
    }

    protected abstract RedisEvent listen();

    protected Jedis jedis;
}
