package databus.receiver.redis;

import java.util.List;

import databus.event.mysql.Column;
import redis.clients.jedis.Jedis;

public interface ColumnModificationListener {
    
    public void insert(Jedis jedis, List<Column> primaryKeyValues, Column column);
    
    public void update(Jedis jedis, List<Column> primaryKeyValues, Column column);
    
    public void delete(Jedis jedis, List<Column> primaryKeyValues, Column column);

}
