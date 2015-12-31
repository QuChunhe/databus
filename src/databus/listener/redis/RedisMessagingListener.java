package databus.listener.redis;

import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.event.RedisEvent;
import databus.event.redis.RedisMessaging;

public class RedisMessagingListener extends RedisListener {    

    public RedisMessagingListener() {
        super();
    }

    @Override
    public void initialize(Properties properties) {
        super.initialize(properties);
        String rawKeys = properties.getProperty("redis.keys");
        if ((null==rawKeys) || (rawKeys.length()==0)) {
            log.error("key element is null for RedisMessageListener");
            System.exit(1);
        }
        keys = rawKeys.split(",");
    }

    @Override
    protected RedisEvent listen() {
        List<String> result = jedis.blpop(0, keys);
        if ((null==result) || (result.size()==0))  {
            log.error("Has received null result");
            return null;
        }
        String key = result.get(0);
        String message = result.get(1);
        
        RedisMessaging event = new RedisMessaging(key, message);        
        return event;
    } 

    private static Log log = LogFactory.getLog(RedisMessagingListener.class);
    
    private String[] keys;
}
