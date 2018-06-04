package databus.receiver.cassandra;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.event.MysqlEvent;

/**
 * Created by Qu Chunhe on 2018-06-01.
 */
public class MysqlBatchCassandraBean extends BatchCassandraBean {
    public MysqlBatchCassandraBean() {
        super();
    }

    @Override
    protected String toKey(Event event) {
        if (event instanceof MysqlEvent) {
            MysqlEvent mysqlEvent = (MysqlEvent) event;
            return mysqlEvent.database() + "." + mysqlEvent.table();
        }
        log.error("Is not a MysqlEvent : "+event.toString());
        return null;
    }

    private final static Log log = LogFactory.getLog(MysqlBatchCassandraBean.class);

}
