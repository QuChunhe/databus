package databus.receiver.redis2;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import databus.util.RedisClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.event.mysql.*;

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
    public void receive(final RedisClient redisClient, final Event event) {
        if (event instanceof AbstractMysqlWriteRow) {
            AbstractMysqlWriteRow e = (AbstractMysqlWriteRow) event;
            Table table = getTable(e.table().toLowerCase());
            if (null == table) {
                return;
            }
            List<Column> primaryKeyColumns = e.primaryKeys();
            List<Column> row = e.row();
            if (e instanceof MysqlUpdateRow) {
                table.update(redisClient, primaryKeyColumns, row);
            } else if (e instanceof MysqlInsertRow) {
                table.insert(redisClient, primaryKeyColumns, row);
            } else if (e instanceof MysqlDeleteRow) {
                table.delete0(redisClient, primaryKeyColumns, row);
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
