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
        StringBuilder builder = new StringBuilder(128);
        builder.append(source())
               .append(":")
               .append(type())
               .append(" = ")
               .append("{")
               .append("\"time\": ")
               .append(time())
               .append(", ")
               .append("\"source\": \"")
               .append(source())
               .append("\", ")
               .append("\"key\": \"")
               .append(key())
               .append("\", ")
               .append("\"value\": \"")
               .append(message)
               .append("\"}");
        return builder.toString();
    }
    
    private String message;
}
