package databus.receiver.redis;

import java.util.*;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import databus.event.mysql.Column;
import databus.event.mysql.ColumnComparator;

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
        insert(transaction, primaryKeys, toMap(row));
        transaction.exec();
    }
    
    public void delete(Jedis jedis, List<Column> primaryKeys, List<Column> row) {
        Transaction transaction = jedis.multi();
        delete(transaction, primaryKeys, row);
        transaction.exec();
    }
    
    public void update(Jedis jedis, List<Column> primaryKeys, List<Column> row) {
        Set<String> primaryKeyNameSet = toMap(primaryKeys).keySet();
        List<Column> newPrimaryKeys = new LinkedList();
        for(Column c : row) {
            if (primaryKeyNameSet.contains(c.name())) {
                newPrimaryKeys.add(c);
            }
        }

        if (newPrimaryKeys.size() > 0) {
            Set<String> newPrimaryKeyNameSet = toMap(newPrimaryKeys).keySet();
            for(Column c : primaryKeys) {
                if (!newPrimaryKeyNameSet.contains(c.name())) {
                    newPrimaryKeys.add(c);
                }
            }

            Map<String, String> oldRow = jedis.hgetAll(getRedisKey(primaryKeys));
            Map<String, String> newRow = new HashMap(oldRow);
            for(Column c : row) {
                if (null==replicatedColumns || replicatedColumns.contains(c.name().toLowerCase())) {
                    newRow.put(c.name(), null==c.value() ? "" : c.value());
                }
            }
            Transaction transaction = jedis.multi();
            replace(transaction, primaryKeys, oldRow, newPrimaryKeys, newRow);
            transaction.exec();
        } else {
            update(jedis, primaryKeys, toMap(row));
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
            builder.append(c.name())
                   .append("=")
                   .append(c.value()==null ? "" : c.value())
                   .append("&");
        }
        
        return builder.substring(0, builder.length()-1);
    }

    public String name() {
        return name;
    }

    protected Map<String, String> toMap(List<Column> row) {
        HashMap<String, String> columns = new HashMap(row.size());
        for(Column c : row) {
            if (null==replicatedColumns || replicatedColumns.contains(c.name().toLowerCase())) {
                String value = null!=c.value() ? c.value() : "";
                columns.put(c.name(), value);
            }
        }
        
        return columns;
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

    protected void update(Jedis jedis, List<Column> primaryKeys, Map<String, String> value) {
        if (value.size() > 0) {
            jedis.hmset(getRedisKey(primaryKeys), value);
        }
    }

    protected static final ColumnComparator COLUMN_COMPARATOR = new ColumnComparator();
    protected String name;
    protected String system = "database";

    private Set<String> replicatedColumns = null;
}
