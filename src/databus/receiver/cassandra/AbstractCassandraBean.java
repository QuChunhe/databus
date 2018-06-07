package databus.receiver.cassandra;

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
        }
    }

    protected abstract String toSql(Event event);
}
