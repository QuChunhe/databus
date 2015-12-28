package databus.event.redis;


import databus.event.RedisEvent;

public class RedisMessage  extends AbstractRedisEvent{
    
    public RedisMessage() {
        this(null, null);
    }

    public RedisMessage(String key, String message) {
        super(key);
        this.message = message;
    }

    @Override
    public String type() {
        return RedisEvent.Type.LIST_MESSAGES.toString();
    }
    
    public String message() {
        return message;
    }    
    
    @Override
    public String toString() {
        return  type()+" from "+ key()+" : "+message;
    }
    
    private String message;

}
