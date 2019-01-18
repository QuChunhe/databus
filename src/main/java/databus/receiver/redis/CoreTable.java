package databus.receiver.redis;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import databus.event.mysql.Column;
import redis.clients.jedis.Jedis;

public class CoreTable extends Table{
    
    public static final String SEPERATOR = "|^|";

    public CoreTable(String system, String name) {
        super(system, name);
    }

    @Override
    public void insert(Jedis jedis, List<Column> primaryKeys, List<Column> row) {
        super.insert(jedis, primaryKeys, row);
    }

    @Override
    public void delete(Jedis jedis, List<Column> primaryKeys, List<Column> row) {
        super.delete(jedis, primaryKeys, row);
    }

    @Override
    public void update(Jedis jedis, List<Column> primaryKeys, List<Column> row) {
        super.update(jedis, primaryKeys, row);
    }
    
    public void insertFields(Jedis jedis, Map<String, String> coreTablePrimaryKeys, 
                             Map<String, String> associationTableFields) {
        String redisKey = getCoreTableRedisKey(coreTablePrimaryKeys);
        jedis.hmset(redisKey, associationTableFields);        
    }
    
    public void updateFields(Jedis jedis, Map<String, String> coreTablePrimaryKeys, 
                             Map<String, String> associationTableFields) {
        insertFields(jedis, coreTablePrimaryKeys, associationTableFields);
    }
    
    public void deleteFields(Jedis jedis, Map<String, String> coreTablePrimaryKeys, 
                             Map<String, String> associationTableFields) {
        String redisKey = getCoreTableRedisKey(coreTablePrimaryKeys);
        String[] fields = associationTableFields.keySet().toArray(STRING_ARRAY);
        jedis.hdel(redisKey, fields);
    }
    
    private String getCoreTableRedisKey(Map<String, String> coreTablePrimaryKeys) {
        String[] orderedPrimaryKeys = coreTablePrimaryKeys.keySet().toArray(STRING_ARRAY);
        Arrays.sort(orderedPrimaryKeys);
        StringBuilder builder = new StringBuilder(128);
        builder.append(system)
               .append(":")
               .append(name)
               .append(":");
        for(int i=0; i<orderedPrimaryKeys.length; i++) {
            if (i > 0) {
                builder.append("&");
            }
            String key = orderedPrimaryKeys[i];
            builder.append(key)
                   .append("=")
                   .append(coreTablePrimaryKeys.get(key));
        }
        builder.append(":all");
        return builder.toString();
    }
    
    private static final String[] STRING_ARRAY = new String[1];
}
