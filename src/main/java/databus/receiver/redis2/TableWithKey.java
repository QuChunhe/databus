package databus.receiver.redis2;

import java.util.*;

import databus.util.RedisClient;
import redis.clients.jedis.Transaction;

import databus.event.mysql.Column;

/**
 * Created by Qu Chunhe on 2019-09-24.
 */
public class TableWithKey extends Table {

    public TableWithKey(String name) {
        super(name);
    }

    public TableWithKey(String system, String name) {
        super(system, name);
    }

    public void setKeys(Collection<String> keys) {
        keySet = new HashSet<>(keys);
        keyArray = keySet.toArray(new String[keys.size()]);
        Arrays.sort(keyArray);
    }

    @Override
    protected void insert0(Transaction transaction, List<Column> primaryKeys,
                           Map<String, String> row) {
        super.insert0(transaction, primaryKeys, row);
        transaction.sadd(getRedisKeyFromRow(row), getRedisValue(primaryKeys));
    }

    protected void insert0(RedisClient redisClient, List<Column> primaryKeys,
                           Map<String, String> row) {
        super.insert0(redisClient, primaryKeys, row);
        redisClient.sadd(getRedisKeyFromRow(row), getRedisValue(primaryKeys));
    }

    @Override
    protected void delete0(Transaction transaction, List<Column> primaryKeys, List<Column> row) {
        super.delete0(transaction, primaryKeys, row);
        transaction.srem(getRedisKeyFromRow(transform(row).first), getRedisValue(primaryKeys));
    }

    protected void delete0(RedisClient redisClient, List<Column> primaryKeys, List<Column> row) {
        super.delete0(redisClient, primaryKeys, row);
        redisClient.srem(getRedisKeyFromRow(transform(row).first), getRedisValue(primaryKeys));
    }

    @Override
    protected void updateWithPrimaryKeys(Transaction transaction,
                                         List<Column> oldPrimaryKeys, Map<String, String> oldRow,
                                         List<Column> newPrimaryKeys, Map<String, String> newRow) {
        deleteOldRow(transaction, oldPrimaryKeys, oldRow);
        insert0(transaction, newPrimaryKeys, newRow);
    }

    @Override
    protected void updateWithPrimaryKeys(RedisClient redisClient,
                                         List<Column> oldPrimaryKeys, Map<String, String> oldRow,
                                         List<Column> newPrimaryKeys, Map<String, String> newRow) {
        redisClient.del(getRedisKey(oldPrimaryKeys));
        redisClient.srem(getRedisKeyFromRow(oldRow), getRedisValue(oldPrimaryKeys));
        insert0(redisClient, newPrimaryKeys, newRow);
    }

    @Override
    protected void updateWithoutPrimaryKeys(RedisClient redisClient, List<Column> primaryKeys, Map<String,
                                            String> columnMap, List<String> nullColumns) {
        Map<String, String> updatedKeyMap = new HashMap();
        for(Map.Entry<String, String> e : columnMap.entrySet()) {
            if (keySet.contains(e.getKey())) {
                updatedKeyMap.put(e.getKey(), e.getValue());
            }
        }
        for(String c : nullColumns) {
            if (keySet.contains(c)) {
                updatedKeyMap.put(c, null);
            }
        }
        String redisKey = getRedisKey(primaryKeys);
        Map<String, String> oldKeyMap = null;
        if (updatedKeyMap.size() > 0) {
            List<String> valueList = redisClient.hmget(redisKey, keyArray);
            oldKeyMap = new HashMap();
            Set<String> updatedKeySet = updatedKeyMap.keySet();
            for(int i=0, len=keyArray.length; i<len; i++){
                String k = keyArray[i];
                String v = valueList.get(i);
                oldKeyMap.put(k, v);
                if (!updatedKeySet.contains(k)) {
                    updatedKeyMap.put(k, v);
                }
            }
        }

        if (redisClient.doesSupportTransaction()) {
            Transaction transaction = redisClient.multi();
            super.updateRow(transaction, redisKey, columnMap, nullColumns);
            if (updatedKeyMap.size() > 0) {
                String value = getRedisValue(primaryKeys);
                transaction.srem(getRedisKeyFromRow(oldKeyMap), value);
                transaction.sadd(getRedisKeyFromRow(updatedKeyMap), value);
            }
            transaction.exec();
        } else {
            super.updateRow(redisClient, redisKey, columnMap, nullColumns);
            if (updatedKeyMap.size() > 0) {
                String value = getRedisValue(primaryKeys);
                redisClient.srem(getRedisKeyFromRow(oldKeyMap), value);
                redisClient.sadd(getRedisKeyFromRow(updatedKeyMap), value);
            }
        }
    }

    private String getRedisValue(List<Column> primaryKeys) {
        Column[] sortedPrimaryKeys = primaryKeys.toArray(new Column[primaryKeys.size()]);
        Arrays.sort(sortedPrimaryKeys, COLUMN_COMPARATOR);
        StringBuilder builder = new StringBuilder(128);
        for(Column c : sortedPrimaryKeys) {
            if (c.value()!=null) {
                builder.append(c.name())
                        .append("=")
                        .append(c.value())
                        .append("<&>");
            }
        }
        return builder.length()>0 ? builder.substring(0, builder.length()-3) : "";
    }

    private void deleteOldRow(Transaction transaction, List<Column> primaryKeys,
                              Map<String, String> row) {
        transaction.del(getRedisKey(primaryKeys));
        transaction.srem(getRedisKeyFromRow(row), getRedisValue(primaryKeys));
    }

    private String getRedisKeyFromRow(Map<String, String> row) {
        StringBuilder builder = new StringBuilder(128);
        builder.append(system)
               .append(":")
               .append(name)
               .append(":");
        for(String k : keyArray) {
            String v = row.get(k);
            if (null != v) {
                builder.append(k)
                       .append("=")
                       .append(v)
                       .append("&");
            }
        }

        return builder.charAt(builder.length()-1)=='&' ? builder.substring(0, builder.length()-1) :
                                                         builder.toString();
    }

    private Set<String> keySet;
    private String[] keyArray;
}
