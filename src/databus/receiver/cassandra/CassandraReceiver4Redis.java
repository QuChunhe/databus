package databus.receiver.cassandra;

import databus.core.Event;

/**
 * Created by Qu Chunhe on 2018-06-01.
 */
public class CassandraReceiver4Redis extends CassandraReceiver {
    public CassandraReceiver4Redis() {
        super();
    }

    @Override
    protected void receive(CassandraConnection conn, Event event) {

    }
}
