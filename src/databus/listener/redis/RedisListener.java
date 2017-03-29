package databus.listener.redis;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import redis.clients.jedis.Jedis;

import databus.event.RedisEvent;
import databus.listener.RunnableListener;

public abstract class RedisListener extends RunnableListener {

    public RedisListener(String name) {
        super();
    }
    
    public RedisListener() {
        this("RedisListener");
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    @Override
    protected ListeningRunner createListeningRunner() {
        return new RedisListeningRunner();
    }

    protected abstract RedisEvent listen();

    private void newJedis() {
        jedis = new Jedis(host, port, timeout);
    }

    protected Jedis jedis;
    
    private final static Log log = LogFactory.getLog(RedisListener.class);
    
    private String host = "127.0.0.1";
    private int port = 6379;
    private int timeout = 1000;
    
    private class RedisListeningRunner extends ListeningRunner {        

        @Override
        public void runOnce() throws Exception {
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
            newJedis();
        }

        @Override
        public void processFinally() {
        }

        @Override
        public void stop(Thread owner) {
            try {
                log.info("Waiting RedisListener finish!");
                owner.join();
            } catch (InterruptedException e) {
                //do nothing
            }
        }

        @Override
        public void close() {
            jedis.close();
        }        
    }
}
