package databus.listener.redis;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.event.RedisEvent;
import databus.listener.RunnableListener;
import redis.clients.jedis.Jedis;

public abstract class RedisListener extends RunnableListener {

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
    }

    @Override
    protected ListeningRunner createListeningRunner() {
        return new RedisRunner();
    }

    protected abstract RedisEvent listen();
    
    private void newJedis() {
        jedis = new Jedis(host, port, timeout);
    }

    protected Jedis jedis;
    
    private static Log log = LogFactory.getLog(RedisListener.class);
    
    private String host;
    private int port;
    private int timeout;
    
    private class RedisRunner extends ListeningRunner {        

        @Override
        public void runOnce() {
            RedisEvent event = listen();
            if (null != event) {
               onEvent(event); 
            } 
            super.runOnce();
        }

        @Override
        public void processException(Exception e) {
            super.processException(e);
            jedis.close();
            newJedis();
        }

        @Override
        public void initialize() {
            
        }

        @Override
        public void stop(Thread owner) {
            try {
                log.info("Waiting RedisListener finish!");
                owner.join();
            } catch (InterruptedException e) {
            }
        }

        @Override
        public void close() {
            jedis.close();
            
        }        
    }
}
