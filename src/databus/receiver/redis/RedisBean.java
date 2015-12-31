package databus.receiver.redis;

import databus.receiver.Bean;
import redis.clients.jedis.Jedis;

public interface RedisBean extends Bean {
    
    public void operate(Jedis jedis);

}
