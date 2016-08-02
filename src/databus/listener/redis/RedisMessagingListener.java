package databus.listener.redis;

import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.event.RedisEvent;
import databus.event.redis.RedisMessaging;

public class RedisMessagingListener extends RedisListener {    

    public RedisMessagingListener() {
        super("RedisMessagingListener");
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
        for(int i=0; i<keys.length; i++) {
            keys[i] = keys[i].trim();
        }
    }

    @Override
    protected RedisEvent listen() {
        List<String> result = jedis.blpop(2, keys);
        if ((null==result) || (result.size()==0))  {
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
