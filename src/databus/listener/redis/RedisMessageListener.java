package databus.listener.redis;

import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.event.RedisEvent;
import databus.event.redis.RedisMessage;

public class RedisMessageListener extends RedisListener {    

    public RedisMessageListener() {
        super();
    }

    @Override
    public void initialize(Properties properties) {
        super.initialize(properties);
        key = properties.getProperty("redis.key");
        if ((null==key) || (key.length()==0)) {
            log.error("key element is null for RedisMessageListener");
            System.exit(1);
        }
    }

    @Override
    protected RedisEvent listen() {
        List<String> result = jedis.blpop(0, key);
        if ((null==result) || (result.size()==0))  {
            log.error("Has received null result");
            return null;
        }
        if (result.size() != 2) {
            log.error("Received Result has "+result.size()+" elements:"+result.toString());
            return null;
        }        
        if (!key.equals(result.get(0))) {
            log.error("Expect key "+key+" : "+result.toString());
        }
        
        RedisMessage event = new RedisMessage(key, result.get(1));        
        return event;
    } 

    private static Log log = LogFactory.getLog(RedisMessageListener.class);
    
    private String key;
}
