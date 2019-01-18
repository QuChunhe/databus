package databus.receiver.redis;

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import redis.clients.jedis.Jedis;

import databus.core.Event;
import databus.event.mysql.AbstractMysqlWriteRow;
import databus.event.mysql.Column;
import databus.event.mysql.MysqlDeleteRow;
import databus.event.mysql.MysqlInsertRow;
import databus.event.mysql.MysqlUpdateRow;

public class RedisSlave4Mysql extends RedisReceiver {

    public RedisSlave4Mysql() {
        super();
        tableMap = new HashMap<>();
    }

    public void setTables(Collection<Table> tables) {
        for(Table t : tables) {
            tableMap.put(t.name(), t);
        }
    }

    public void setSystem(String system) {
        this.system = system;
    }

    @Override
    protected void receive(Jedis jedis, Event event) {
        if (event instanceof AbstractMysqlWriteRow) {
            AbstractMysqlWriteRow e = (AbstractMysqlWriteRow) event;
            Table table = getTable(e.table().toLowerCase());
            if (null == table) {
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
            } else {
                log.info(event.toString());
            }
        } else {
            log.warn(event.getClass().getName()+" is not AbstractMysqlWriteRow : " +
                     event.toString());
        }
    }

    protected Table getTable(String tableName) {
        Table table = tableMap.get(tableName);
        if (null == table) {
            synchronized (lock) {
                table = tableMap.get(tableName);
                if (null == table) {
                    table = new Table(system, tableName);
                    tableMap.put(tableName, table);
                }
            }
        }
        return table;
    }

    private final static Log log = LogFactory.getLog(RedisSlave4Mysql.class);

    private final Map<String, Table> tableMap;
    private final Object lock = new Object();

    private String system = "database";
}
