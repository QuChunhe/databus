package databus.receiver.cassandra;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;

/**
 * Created by Qu Chunhe on 2018-06-04.
 */
public abstract class AbstractCassandraBean implements CassandraBean {

    @Override
    public void execute(CassandraConnection conn, Event event) {
        String sql = toSql(event);
        if (null != sql) {
            conn.insertAsync(sql, new LogFailureCallback(sql));
        } else {
            log.error("Can not convert to sql " + event.toString());
        }
    }

    protected abstract String toSql(Event event);

    private final static Log log = LogFactory.getLog(AbstractCassandraBean.class);
}
