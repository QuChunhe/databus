package databus.receiver.redis;

import java.util.*;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import databus.event.mysql.Column;

/**
 * Created by Qu Chunhe on 2019-01-10.
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
    protected void insert(Transaction transaction, List<Column> primaryKeys,
                          Map<String, String> row) {
        super.insert(transaction, primaryKeys, row);
        transaction.sadd(getRedisKeyFromRow(row), getRedisValue(primaryKeys));
    }

    @Override
    protected void delete(Transaction transaction, List<Column> primaryKeys, List<Column> row) {
        delete(transaction, primaryKeys, transform(row).first);
    }

    @Override
    protected void replace(Transaction transaction,
                           List<Column> oldPrimaryKeys, Map<String, String> oldRow,
                           List<Column> newPrimaryKeys, Map<String, String> newRow) {
        delete(transaction, oldPrimaryKeys, oldRow);
        insert(transaction, newPrimaryKeys, newRow);
    }

    @Override
    protected void update(Jedis jedis, List<Column> primaryKeys,  Map<String, String> columnMap,
                          List<String> nullColumns) {
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
            List<String> valueList = jedis.hmget(redisKey, keyArray);
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

        Transaction transaction = jedis.multi();
        super.update(transaction, redisKey, columnMap, nullColumns);
        if (updatedKeyMap.size() > 0) {
            String value = getRedisValue(primaryKeys);
            transaction.srem(getRedisKeyFromRow(oldKeyMap), value);
            transaction.sadd(getRedisKeyFromRow(updatedKeyMap), value);
        }
        transaction.exec();
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

    private void delete(Transaction transaction, List<Column> primaryKeys,
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

        return builder.charAt(builder.length()-1)=='&' ?
                builder.substring(0, builder.length()-1) :
                builder.toString();

    }

    private Set<String> keySet;
    private String[] keyArray;
}
