package databus.receiver.redis;

import java.util.List;

import databus.event.mysql.Column;
import redis.clients.jedis.Jedis;

public interface ColumnModificationListener {
    
    void insert(Jedis jedis, List<Column> primaryKeyValues, Column column);
    
    void update(Jedis jedis, List<Column> primaryKeyValues, Column column);
    
    void delete(Jedis jedis, List<Column> primaryKeyValues, Column column);

}
