package databus.listener.redis;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.event.RedisEvent;
import databus.event.redis.RedisMessages;

public class RedisMessageListener extends RedisListener {    

    public RedisMessageListener() {
        super();
    }

    @Override
    public void initialize(Properties properties) {
        super.initialize(properties);
        key = properties.getProperty("key");
        if ((null==key) || (key.length()==0)) {
            log.error("key element is null for RedisMessageListener");
            System.exit(1);
        }
        maxLength = Integer.parseInt(properties.getProperty("maxLength", "1"));
    }

    @Override
    protected Collection<RedisEvent> listen() {
        List<String> messages = jedis.blpop(0, key);
        int len = messages.size();        
        int fromIndex = 0;
        int toIndex = maxLength; 
        LinkedList<RedisEvent> eventList = new LinkedList<RedisEvent>();
        while(toIndex < len) {
            fromIndex = toIndex;
            toIndex = toIndex + maxLength;
            toIndex = (toIndex<=len) ? toIndex : len;
            RedisMessages e = new RedisMessages(key, messages.subList(fromIndex, toIndex));
            eventList.add(e);
        }
        return eventList;
    }    
    
    private static Log log = LogFactory.getLog(RedisMessageListener.class);
    
    private String key;
    private int maxLength;
}
