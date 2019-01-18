package databus.receiver.cassandra;

import databus.core.Event;
import databus.event.MysqlEvent;
import databus.event.RedisEvent;

/**
 * Created by Qu Chunhe on 2018-06-08.
 */
public class CassandraBean4RedisAndMysql implements CassandraBean {

    public CassandraBean4RedisAndMysql() {
    }

    @Override
    public void execute(CassandraConnection conn, Event event) {
        if (event instanceof MysqlEvent) {
            cassandraBean4Mysql.execute(conn, event);
        } else if (event instanceof RedisEvent) {
            cassandraBean4Redis.execute(conn, event);
        }
    }

    public void setCassandraBean4Mysql(CassandraBean cassandraBean4Mysql) {
        this.cassandraBean4Mysql = cassandraBean4Mysql;
    }

    public void setCassandraBean4Redis(CassandraBean cassandraBean4Redis) {
        this.cassandraBean4Redis = cassandraBean4Redis;
    }

    private CassandraBean cassandraBean4Mysql;
    private CassandraBean cassandraBean4Redis;
}
