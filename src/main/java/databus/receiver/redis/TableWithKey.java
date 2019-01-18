package databus.receiver.redis;

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
        delete(transaction, primaryKeys, toMap(row));
    }

    @Override
    protected void replace(Transaction transaction,
                           List<Column> oldPrimaryKeys, Map<String, String> oldRow,
                           List<Column> newPrimaryKeys, Map<String, String> newRow) {
        delete(transaction, oldPrimaryKeys, oldRow);
        insert(transaction, newPrimaryKeys, newRow);
    }

    @Override
    protected void update(Jedis jedis, List<Column> primaryKeys, Map<String, String> columnMap) {
        super.update(jedis, primaryKeys, columnMap);
        Map<String, String> newKeyMap = new HashMap();
        for(Map.Entry<String, String> e : columnMap.entrySet()) {
            if (keySet.contains(e.getKey())) {
                newKeyMap.put(e.getKey(), e.getValue());
            }
        }
        Map<String, String> oldKeyMap = null;
        if (newKeyMap.size() > 0) {
            List<String> valueList = jedis.hmget(getRedisKey(primaryKeys), keyArray);
            oldKeyMap = new HashMap();
            for(int i=0, len=keyArray.length; i<len; i++){
                oldKeyMap.put(keyArray[i], valueList.get(i));
            }
            for(String k : keyArray) {
                if (null == newKeyMap.get(k)) {
                    String v = oldKeyMap.get(k);
                    newKeyMap.put(k,  null==v ? "" : v);
                }
            }
        }

        Transaction transaction = jedis.multi();
        if (columnMap.size() > 0) {
            transaction.hmset(getRedisKey(primaryKeys), columnMap);
        }
        if (newKeyMap.size() > 0) {
            String value = getRedisValue(primaryKeys);
            transaction.srem(getRedisKeyFromRow(oldKeyMap), value);
            transaction.sadd(getRedisKeyFromRow(newKeyMap), value);

        }
        transaction.exec();
    }

    private String getRedisValue(List<Column> primaryKeys) {
        Column[] sortedPrimaryKeys = primaryKeys.toArray(new Column[primaryKeys.size()]);
        Arrays.sort(sortedPrimaryKeys, COLUMN_COMPARATOR);
        StringBuilder builder = new StringBuilder(128);
        for(Column c : sortedPrimaryKeys) {
            builder.append(c.name())
                   .append("=")
                   .append(c.value()==null ? "" : c.value())
                   .append("&");
        }
        return builder.substring(0, builder.length()-1);
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
            if (null == v) {
                log.error("Key "+k+" is NULL");
            }

            builder.append(k)
                   .append("=")
                   .append(v==null ? "" : v)
                   .append("&");
        }

        return builder.substring(0, builder.length()-1);

    }

    private final static Log log = LogFactory.getLog(TableWithKey.class);
    private Set<String> keySet;
    private String[] keyArray;
}
