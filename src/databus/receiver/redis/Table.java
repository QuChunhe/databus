package databus.receiver.redis;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import databus.event.mysql.Column;
import redis.clients.jedis.Jedis;

public class Table {

    public Table(String system, String name) {        
        this.name = name.toLowerCase();
        this.system = system.toLowerCase();
    }    
    
    public void setReplicatedColumn(String column) {
        replicatedColumns.add(column);
    }
    
    public void insert(Jedis jedis, List<Column> primaryKeys, List<Column> row) {
        String redisKey = getRedisKey(primaryKeys);
        Map<String, String> columns = getRelicatedColumns(row);
        if (columns.size() > 0) {
            jedis.hmset(redisKey, columns);
        }        
    }
    
    public void delete(Jedis jedis, List<Column> primaryKeys, List<Column> row) {
        String redisKey = getRedisKey(primaryKeys);
        jedis.del(redisKey);
    }
    
    public void update(Jedis jedis, List<Column> primaryKeys, List<Column> row) {
        insert(jedis, primaryKeys, row);
    }
    
    public String getRedisKey(List<Column> primaryKeys) {
        Column[] orderedPrimaryKeys = primaryKeys.toArray(COLUMN_ARRAY);
        Arrays.sort(orderedPrimaryKeys, COLUMN_COMPARATOR);
        
        StringBuilder builder = new StringBuilder(128);
        builder.append(system)
               .append(":")
               .append(name)
               .append(":");
        for(Column c : orderedPrimaryKeys) {
            builder.append(c.name())
                   .append("=")
                   .append(c.value())
                   .append("&");
        }
        
        return builder.substring(0, builder.length()-1);
    }
    
    private Map<String, String> getRelicatedColumns(List<Column> row) {
        LinkedHashMap<String, String> columns = new LinkedHashMap<String, String>();
        for(Column c : row) {
            if (replicatedColumns.isEmpty() || replicatedColumns.contains(c.name())) {
                String value = null!=c.value() ? c.value() : "";
                columns.put(c.name(), value);
            }
        }
        
        return columns;
    }

    protected String name;
    protected String system;
    
    private static final ColumnComparator COLUMN_COMPARATOR = new ColumnComparator();
    private static final Column[] COLUMN_ARRAY = new Column[1];    
    
    private Set<String> replicatedColumns = new HashSet<String>();
    
    private static class ColumnComparator implements Comparator<Column> {

        @Override
        public int compare(Column c1, Column c2) {
            return c1.name().compareTo(c2.name());
        }        
    }
}
