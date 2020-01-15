package databus.util;

import java.util.*;

/**
 * Created by Qu Chunhe on 2020-01-14.
 */
public class RedisCache4Mysql {

    public RedisCache4Mysql() {
    }

    public RedisCache4Mysql(String system) {
        setSystem(system);
    }

    public void setRedisClient(RedisClient redisClient) {
        this.redisClient = redisClient;
    }

    public void setSystem(String system) {
        this.system = system;
    }

    public Map<String, String> getRowByPrimaryKeys(String table,
                                                   Map<String, String> primaryKeys) {
        return redisClient.hgetAll(toRedisKey(table, primaryKeys));
    }

    public Map<String, String> getRowByKeys(String table, Map<String, String> keys) {
        Set<String> primaryKeySet = redisClient.smembers(toRedisKey(table, keys));
        if(null != primaryKeySet) {
            for (String primaryKey : primaryKeySet) {
                Map<String, String> row = redisClient.hgetAll(toRedisKey(table, primaryKey));
                if(!row.isEmpty()) {
                    return row;
                }
            }
        }
        return new HashMap<>();
    }

    public Collection<Map<String, String>> getRowsByKeys(String table,
                                                         Map<String, String> keys, int limit) {
        Collection<Map<String,String>> rows =  new LinkedList<>();
        Set<String> primaryKeySet = redisClient.smembers(toRedisKey(table, keys));
        if(null != primaryKeySet) {
            int count = 0;
            for (String primaryKey : primaryKeySet) {
                Map<String, String> row = redisClient.hgetAll(toRedisKey(table, primaryKey));
                if(!row.isEmpty()) {
                    count++;
                    rows.add(row);
                }
                if (count >= limit) {
                    break;
                }
            }
        }
        return rows;
    }

    public String[] getValuesByPrimaryKeys(String table, Map<String,String> primaryKeys,
                                           String[] columns) {
        List<String> value = redisClient.hmget(toRedisKey(table, primaryKeys), columns);
        return null==value || value.size()==0 ? null : value.toArray(new String[columns.length]);
    }

    public String getValueByPrimaryKeys(String table, Map<String, String> primaryKeys,
                                         String column) {
        return redisClient.hget(toRedisKey(table, primaryKeys), column);
    }

    public Collection<String[]> getValuesByKeys(String table, Map<String,String> keys,
                                                String[] columns) {
        Collection<String[]> values = new LinkedList<>();
        Set<String> primaryKeySet = redisClient.smembers(toRedisKey(table, keys));
        if(null != primaryKeySet) {
            for (String primaryKey : primaryKeySet) {
                List<String> v = redisClient.hmget(toRedisKey(table, primaryKey), columns);
                if(null!=v && v.size()>0) {
                    values.add(v.toArray(new String[columns.length]));
                }
            }
        }
        return values;
    }

    public String getValueByKeys(String table, Map<String, String> keys, String column) {
        Set<String> primaryKeySet = redisClient.smembers(toRedisKey(table, keys));
        if(null != primaryKeySet) {
            for (String primaryKey : primaryKeySet) {
                String value = redisClient.hget(toRedisKey(table, primaryKey), column);
                if(null != value) {
                    return value;
                }
            }
        }
        return null;
    }

    private String toRedisKey(String table, Map<String, String> keys) {
        StringBuilder builder = new StringBuilder(128);
        builder.append(system)
               .append(":")
               .append(table)
               .append(":");
        Map<String, String> sortMap = new TreeMap<>();
        sortMap.putAll(keys);
        for(Map.Entry<String, String> entry : sortMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (null != value) {
                builder.append(key)
                       .append("=")
                       .append(value)
                       .append("&");
            }
        }
        return builder.charAt(builder.length()-1)=='&' ?
               builder.substring(0, builder.length()-1) :
               builder.toString();
    }

    private String toRedisKey(String table, String keys) {
        StringBuilder builder = new StringBuilder(128);
        builder.append(system)
               .append(":")
               .append(table)
               .append(":")
               .append(keys.replace("<&>", "&"));
        return builder.toString();
    }

    private RedisClient redisClient;
    private String system = "adm";
}
