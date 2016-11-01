package databus.receiver.redis;

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.event.mysql.Column;
import databus.event.mysql.ColumnComparator;
import redis.clients.jedis.Jedis;

public class Table {

    public Table(String system, String name) {        
        this.name = name.toLowerCase();
        this.system = system.toLowerCase();
    }    
    
    public void setReplicatedColumn(String column) {
        replicatedColumns.add(column);
    }
    
    public String insert(Jedis jedis, List<Column> primaryKeys, List<Column> row) {
        String redisKey = getRedisKey(primaryKeys);
        Map<String, String> columns = getRelicatedColumns(row);
        if (columns.size() > 0) {
            jedis.hmset(redisKey, columns);
        }
        return "HMSET "+redisKey+" "+columns.toString();
    }
    
    public String delete(Jedis jedis, List<Column> primaryKeys, List<Column> row) {
        String redisKey = getRedisKey(primaryKeys);
        jedis.del(redisKey);
        return "DEL "+redisKey;
    }
    
    public String update(Jedis jedis, List<Column> primaryKeys, List<Column> row) {
        List<Column> updatedPrimaryKeys = new ArrayList<>(primaryKeys.size());
        boolean doesContainPK = false;
        for (Column k :  primaryKeys) {
            Column column = null;
            for (Column c : row) {
                if (k.name().equals(c.name())) {
                    column = c;
                    doesContainPK = true;
                    break;
                }
            }
            if (null == column) {
                updatedPrimaryKeys.add(k);
            } else {
                updatedPrimaryKeys.add(column);
            }
        }

        List<Column> updatedRow = row;
        if (doesContainPK) {
            Map<String, String> fields = jedis.hgetAll(getRedisKey(primaryKeys));
            for (Column c : row) {
                fields.put(c.name(), c.value());
            }
            updatedRow = new LinkedList<>();
            for(String name : fields.keySet()) {
                updatedRow.add(new Column(name, fields.get(name), -1));
            }
            delete(jedis, primaryKeys, row);
        }
        return insert(jedis, updatedPrimaryKeys, updatedRow);
    }
    
    public String getRedisKey(List<Column> primaryKeys) {
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
    private static Log log = LogFactory.getLog(Table.class);
    
    private Set<String> replicatedColumns = new HashSet<String>();
}
