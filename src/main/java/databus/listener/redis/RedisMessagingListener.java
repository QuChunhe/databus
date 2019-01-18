package databus.listener.redis;

import java.util.Collection;
import java.util.List;

import databus.event.RedisEvent;
import databus.event.redis.RedisMessaging;

public class RedisMessagingListener extends RedisListener {

    public RedisMessagingListener(String name) {
        super(name);
    }

    public RedisMessagingListener() {
        this("RedisMessagingListener");
    }

    public void setKeys(Collection<String> keys) {
        this.keys = keys.toArray(new String[keys.size()]);
    }

    @Override
    protected RedisEvent listen() {
        List<String> result = jedis.blpop(listeningTimeout, keys);
        if ((null==result) || (result.size()==0))  {
            return null;
        }
        String key = result.get(0);
        String message = result.get(1);
        return new RedisMessaging(key, message);
    } 

    private String[] keys;
    private int listeningTimeout = 2;
}
