package databus.receiver.redis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.event.mysql.AbstractMysqlWriteRow;
import databus.event.mysql.Column;
import databus.event.mysql.MysqlDeleteRow;
import databus.event.mysql.MysqlInsertRow;
import databus.event.mysql.MysqlUpdateRow;
import redis.clients.jedis.Jedis;

public class RedisSlave4Mysql extends RedisReceiver {

    public RedisSlave4Mysql() {
        super();
        tableMap = new HashMap<String, Table>();
    }

    @Override
    protected void receive(Jedis jedis, Event event) {
        if (event instanceof AbstractMysqlWriteRow) {
            log.info("Have received : "+event.toString());
            AbstractMysqlWriteRow e = (AbstractMysqlWriteRow) event;
            String tableName = e.table().toLowerCase();
            Table table = tableMap.get(tableName);
            if (null == table) {
                log.warn("Has't information about "+tableName);
                return;
            }
            List<Column> primaryKeyColumns = e.primaryKeys();
            List<Column> row = e.row();
            if (e instanceof MysqlUpdateRow) {
                table.update(jedis, primaryKeyColumns, row);
            } else if (e instanceof MysqlInsertRow) {
                table.insert(jedis, primaryKeyColumns, row);
            } else if (e instanceof MysqlDeleteRow) {
                table.delete(jedis, primaryKeyColumns, row);
            }            
        } else {
            log.warn(event.getClass().getName()+" isn't AbstractMysqlWriteRow : " + 
                     event.toString());
        }
    }    
    
    @Override
    public void initialize(Properties properties) {
        super.initialize(properties);
        
        String system = properties.getProperty("system", "system");
        for(String name : properties.stringPropertyNames()) {
            if (name.startsWith(TABLES_PREFIX)) {
                String tableName = name.substring(TABLES_PREFIX.length());
                if (tableName.length() == 0) {
                    continue;
                }
                Table table = new Table(system, tableName);
                tableMap.put(tableName.toLowerCase(), table);
                String value = properties.getProperty(name);
                if ((null != value) && (value.length()>0)) {
                    String[] columns = value.split(",");
                    for(String c : columns) {
                        String columnName = c.trim().toLowerCase();
                        if ((null!=columnName) && (columnName.length()>0)) {
                            table.setReplicatedColumn(columnName);
                        }
                    }
                }
            }
        }
        
    }
    
    private static final String TABLES_PREFIX = "replicatedTables.";

    private static Log log = LogFactory.getLog(RedisSlave4Mysql.class);

    private Map<String, Table> tableMap;
}
