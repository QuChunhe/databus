package databus.receiver.redis2;

import java.util.*;

import databus.util.RedisClient;
import redis.clients.jedis.Transaction;

import databus.event.mysql.Column;
import databus.event.mysql.ColumnComparator;
import databus.util.Tuple2;

/**
 * Created by Qu Chunhe on 2019-09-24.
 */
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

    public void insert(RedisClient redisClient, List<Column> primaryKeys, List<Column> row) {
        if (redisClient.doesSupportTransaction()) {
            Transaction transaction = redisClient.multi();
            insert0(transaction, primaryKeys, transform(row).first);
            transaction.exec();
        } else {
            insert0(redisClient, primaryKeys, transform(row).first);
        }
    }
    
    public void delete(RedisClient redisClient, List<Column> primaryKeys, List<Column> row) {
        if (redisClient.doesSupportTransaction()) {
            Transaction transaction = redisClient.multi();
            delete0(transaction, primaryKeys, row);
            transaction.exec();
        } else {
            delete0(redisClient, primaryKeys, row);;
        }
    }
    
    public void update(RedisClient redisClient, List<Column> primaryKeys, List<Column> row) {
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

            Map<String, String> oldRow = redisClient.hgetAll(getRedisKey(primaryKeys));
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
            if (redisClient.doesSupportTransaction()) {
                Transaction transaction = redisClient.multi();
                updateWithPrimaryKeys(transaction, primaryKeys, oldRow, newPrimaryKeys, newRow);
                transaction.exec();
            } else {
                updateWithPrimaryKeys(redisClient, primaryKeys, oldRow, newPrimaryKeys, newRow);
            }

        } else {
            Tuple2<Map<String, String>, List<String>> result = transform(row);
            updateWithoutPrimaryKeys(redisClient, primaryKeys, result.first, result.second);
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

    protected void insert0(Transaction transaction, List<Column> primaryKeys,
                           Map<String, String> row) {
        transaction.hmset(getRedisKey(primaryKeys), row);
    }

    protected void insert0(RedisClient redisClient, List<Column> primaryKeys,
                           Map<String, String> row) {
        redisClient.hmset(getRedisKey(primaryKeys), row);
    }

    protected void delete0(Transaction transaction, List<Column> primaryKeys, List<Column> row) {
        transaction.del(getRedisKey(primaryKeys));
    }

    protected void delete0(RedisClient redisClient, List<Column> primaryKeys, List<Column> row) {
        redisClient.del(getRedisKey(primaryKeys));
    }

    protected void updateWithPrimaryKeys(Transaction transaction,
                                         List<Column> oldPrimaryKeys, Map<String, String> oldRow,
                                         List<Column> newPrimaryKeys, Map<String, String> newRow) {
        transaction.del(getRedisKey(oldPrimaryKeys));
        transaction.hmset(getRedisKey(newPrimaryKeys), newRow);
    }

    protected void updateWithPrimaryKeys(RedisClient redisClient,
                                         List<Column> oldPrimaryKeys, Map<String, String> oldRow,
                                         List<Column> newPrimaryKeys, Map<String, String> newRow) {
        redisClient.del(getRedisKey(oldPrimaryKeys));
        redisClient.hmset(getRedisKey(newPrimaryKeys), newRow);
    }

    protected void updateRow(Transaction transaction, String redisKey,
                             Map<String, String> columnMap, List<String> nullColumns) {
        if (columnMap.size() > 0) {
            transaction.hmset(redisKey, columnMap);
        }
        if (nullColumns.size() > 0) {
            transaction.hdel(redisKey, nullColumns.toArray(new String[nullColumns.size()]));
        }
    }

    protected void updateRow(RedisClient redisClient, String redisKey,
                             Map<String, String> columnMap, List<String> nullColumns) {
        if (columnMap.size() > 0) {
            redisClient.hmset(redisKey, columnMap);
        }
        if (nullColumns.size() > 0) {
            redisClient.hdel(redisKey, nullColumns.toArray(new String[nullColumns.size()]));
        }
    }

    protected void updateWithoutPrimaryKeys(RedisClient redisClient, List<Column> primaryKeys, Map<String,
                                            String> columnMap, List<String> nullColumns) {
        if (redisClient.doesSupportTransaction()) {
            Transaction transaction = redisClient.multi();
            updateRow(transaction, getRedisKey(primaryKeys), columnMap, nullColumns);
            transaction.exec();
        } else {
            updateRow(redisClient, getRedisKey(primaryKeys), columnMap, nullColumns);
        }

    }

    protected static final ColumnComparator COLUMN_COMPARATOR = new ColumnComparator();
    protected String name;
    protected String system = "database";

    private Set<String> replicatedColumns = null;
}
