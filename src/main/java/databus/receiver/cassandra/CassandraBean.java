package databus.receiver.cassandra;

import databus.core.Event;

/**
 * Created by Qu Chunhe on 2018-06-01.
 */
public interface CassandraBean {

    void execute(CassandraConnection conn, Event event);
}
