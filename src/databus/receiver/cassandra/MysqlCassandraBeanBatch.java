package databus.receiver.cassandra;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.event.MysqlEvent;

/**
 * Created by Qu Chunhe on 2018-06-01.
 */
public class MysqlCassandraBeanBatch implements CassandraBean {
    public MysqlCassandraBeanBatch() {
    }

    public void setMysqlCassandraBeanMap(Map<String, MysqlCassandraBean> mysqlCassandraBeanMap) {
        this.mysqlCassandraBeanMap = mysqlCassandraBeanMap;
    }

    @Override
    public void execute(CassandraConnection conn, Event event) {
        if (event instanceof MysqlEvent) {
            MysqlEvent mysqlEvent = (MysqlEvent) event;
            String fullTableName = mysqlEvent.database() + "." + mysqlEvent.table();
            MysqlCassandraBean mysqlCassandraBean = mysqlCassandraBeanMap.get(fullTableName);
            if (null == mysqlCassandraBean) {
                log.error("Can not get MysqlCassandraBean for "+fullTableName);
            } else {
                mysqlCassandraBean.execute(conn, event);
            }
        } else {
            log.error("Is not a MysqlEvent : "+event.toString());
        }
    }

    private final static Log log = LogFactory.getLog(MysqlCassandraBeanBatch.class);

    private Map<String, MysqlCassandraBean> mysqlCassandraBeanMap;
}
