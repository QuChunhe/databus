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
import databus.util.Benchmark;

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

    @Override
    protected void receive(Jedis jedis, Event event) {
        if (event instanceof AbstractMysqlWriteRow) {
            Benchmark benchmark = new Benchmark();
            AbstractMysqlWriteRow e = (AbstractMysqlWriteRow) event;
            String tableName = e.table().toLowerCase();
            Table table = tableMap.get(tableName);
            if (null == table) {
                log.warn("Has not information about "+tableName);
                return;
            }
            List<Column> primaryKeyColumns = e.primaryKeys();
            List<Column> row = e.row();
            String command = null;
            if (e instanceof MysqlUpdateRow) {
                command = table.update(jedis, primaryKeyColumns, row);
            } else if (e instanceof MysqlInsertRow) {
                command = table.insert(jedis, primaryKeyColumns, row);
            } else if (e instanceof MysqlDeleteRow) {
                command = table.delete(jedis, primaryKeyColumns, row);
            } else {
                log.info(event.toString());
            }
            if (null != command) {
                log.info(benchmark.elapsedMsec(3) +"ms execute : "+command);
            }
        } else {
            log.warn(event.getClass().getName()+" is not AbstractMysqlWriteRow : " +
                     event.toString());
        }
    }    

    private final static Log log = LogFactory.getLog(RedisSlave4Mysql.class);

    private final Map<String, Table> tableMap;
}
