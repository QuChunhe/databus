package databus.event.redis;

import java.util.List;

import databus.event.RedisEvent;

public class RedisMessages  extends AbstractRedisEvent{ 

    public RedisMessages(String key, List<String> messages) {
        super(key);
        this.messages = messages;
    }

    @Override
    public String type() {
        return RedisEvent.Type.LIST_MESSAGES.toString();
    }
    
    public List<String> messages() {
        return messages;
    }
    
    private List<String> messages;

}
