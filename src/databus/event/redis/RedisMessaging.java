package databus.event.redis;


import databus.event.RedisEvent;

public class RedisMessaging  extends AbstractRedisEvent{
    
    public RedisMessaging() {
        this(null, null);
    }

    public RedisMessaging(String key, String message) {
        super(key);
        this.message = message;
        time(System.currentTimeMillis());
    }

    @Override
    public String type() {
        return RedisEvent.Type.LIST_MESSAGING.toString();
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
