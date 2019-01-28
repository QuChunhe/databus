package databus.receiver.redis;

import java.util.*;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import databus.event.mysql.Column;
import databus.event.mysql.ColumnComparator;
import databus.util.Tuple2;

public class Table {

    public Table(String name) {
        this.name = name.toLowerCase();
    }

    public Table(String system, String name) {
        this(name);
        setSystem(system);
    }
    public void setReplicatedColumns(Collection<String> replicatedColumns) {
        this.replicatedColumns = new HashSet<>(replicatedColumns.size());
        for(String c : replicatedColumns) {
            this.replicatedColumns.add(c.toLowerCase());
        }
    }

    public void setSystem(String system) {
        this.system =  system.toLowerCase();
    }

    public void insert(Jedis jedis, List<Column> primaryKeys, List<Column> row) {
        Transaction transaction = jedis.multi();
        insert(transaction, primaryKeys, transform(row).first);
        transaction.exec();
    }
    
    public void delete(Jedis jedis, List<Column> primaryKeys, List<Column> row) {
        Transaction transaction = jedis.multi();
        delete(transaction, primaryKeys, row);
        transaction.exec();
    }
    
    public void update(Jedis jedis, List<Column> primaryKeys, List<Column> row) {
        Set<String> primaryKeyNameSet = toNameSet(primaryKeys);
        List<Column> newPrimaryKeys = new LinkedList();
        for(Column c : row) {
            if (primaryKeyNameSet.contains(c.name())) {
                newPrimaryKeys.add(c);
            }
        }

        if (newPrimaryKeys.size() > 0) {
            Set<String> newPrimaryKeyNameSet = toNameSet(newPrimaryKeys);
            for(Column c : primaryKeys) {
                if (!newPrimaryKeyNameSet.contains(c.name())) {
                    newPrimaryKeys.add(c);
                }
            }

            Map<String, String> oldRow = jedis.hgetAll(getRedisKey(primaryKeys));
            Map<String, String> newRow = new HashMap(oldRow);
            for(Column c : row) {
                if (null==replicatedColumns || replicatedColumns.contains(c.name())) {
                    if (null != c.value()) {
                        newRow.put(c.name(), c.value());
                    } else {
                        newRow.remove(c.name());
                    }
                }
            }
            Transaction transaction = jedis.multi();
            replace(transaction, primaryKeys, oldRow, newPrimaryKeys, newRow);
            transaction.exec();
        } else {
            Tuple2<Map<String, String>, List<String>> result = transform(row);
            update(jedis, primaryKeys, result.first, result.second);
        }
    }
    
    protected String getRedisKey(List<Column> primaryKeys) {
        Column[] sortedPrimaryKeys = primaryKeys.toArray(new Column[primaryKeys.size()]);
        Arrays.sort(sortedPrimaryKeys, COLUMN_COMPARATOR);
        
        StringBuilder builder = new StringBuilder(128);
        builder.append(system)
               .append(":")
               .append(name)
               .append(":");
        for(Column c : sortedPrimaryKeys) {
            if (null != c.value()) {
                builder.append(c.name())
                        .append("=")
                        .append(c.value())
                        .append("&");
            }
        }
        
        return builder.substring(0, builder.length()-1);
    }

    public String name() {
        return name;
    }

    protected Tuple2<Map<String, String>, List<String>> transform(List<Column> row) {
        HashMap<String, String> columnMap = new HashMap(row.size());
        List<String> nullColumns = new LinkedList<>();
        for(Column c : row) {
            if (null==replicatedColumns || replicatedColumns.contains(c.name())) {
                if (null == c.value()) {
                    nullColumns.add(c.name());
                } else {
                    columnMap.put(c.name(), c.value());
                }
            }
        }
        
        return new Tuple2<>(columnMap, nullColumns);
    }

    protected Set<String> toNameSet(List<Column> columns) {
        Set nameSet = new HashSet();
        for(Column c : columns) {
            nameSet.add(c.name());
        }
        return nameSet;
    }

    protected void insert(Transaction transaction, List<Column> primaryKeys,
                          Map<String, String> row) {
        transaction.hmset(getRedisKey(primaryKeys), row);
    }

    protected void delete(Transaction transaction, List<Column> primaryKeys, List<Column> row) {
        transaction.del(getRedisKey(primaryKeys));
    }

    protected void replace(Transaction transaction,
                           List<Column> oldPrimaryKeys, Map<String, String> oldRow,
                           List<Column> newPrimaryKeys, Map<String, String> newRow) {
        transaction.del(getRedisKey(oldPrimaryKeys));
        transaction.hmset(getRedisKey(newPrimaryKeys), newRow);
    }

    protected void update(Transaction transaction, String redisKey,
                          Map<String, String> columnMap, List<String> nullColumns) {
        if (columnMap.size() > 0) {
            transaction.hmset(redisKey, columnMap);
        }
        if (nullColumns.size() > 0) {
            transaction.hdel(redisKey, nullColumns.toArray(new String[nullColumns.size()]));
        }
    }

    protected void update(Jedis jedis, List<Column> primaryKeys, Map<String, String> columnMap,
                          List<String> nullColumns) {
        Transaction transaction = jedis.multi();
        String key = getRedisKey(primaryKeys);
        update(transaction, key, columnMap, nullColumns);
        transaction.exec();
    }

    protected static final ColumnComparator COLUMN_COMPARATOR = new ColumnComparator();
    protected String name;
    protected String system = "database";

    private Set<String> replicatedColumns = null;
}
