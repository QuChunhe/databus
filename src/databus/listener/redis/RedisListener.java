package databus.listener.redis;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.event.RedisEvent;
import databus.listener.AbstractListener;
import redis.clients.jedis.Jedis;

public abstract class RedisListener extends AbstractListener
        implements Runnable {

    public RedisListener() {
        super();
        runner = new Thread(this);
        doRun = false;
    }

    @Override
    public void start() {
        doRun = true;
        runner.start();
    }

    @Override
    public boolean isRunning() {
        return doRun;
    }

    @Override
    public void stop() {
        doRun = false;
        runner.interrupt();

    }

    @Override
    public void initialize(Properties properties) {
        String host = properties.getProperty("redis.host", "127.0.0.1");
        int port = Integer.parseInt(properties.getProperty("redis.port", "6379"));
        int timeout = Integer.parseInt(properties.getProperty("redis.timeout", "60"));

        jedis = new Jedis(host, port, timeout);
    }

    @Override
    public void run() {
        while (doRun) {
            try {
                RedisEvent event = listen();
                if (null != event) {
                   publisher.publish(event); 
                }                
            } catch (Exception e) {
                log.error("Some exceptions happen", e);
            }
        }

    }

    protected abstract RedisEvent listen();

    protected Jedis jedis;

    private static Log log = LogFactory.getLog(RedisListener.class);

    private Thread runner;
    private volatile boolean doRun;
}
