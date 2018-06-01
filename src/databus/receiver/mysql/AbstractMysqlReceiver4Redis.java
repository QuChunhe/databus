package databus.receiver.mysql;

import java.sql.Connection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.event.redis.RedisMessaging;

/**
 * Created by Qu Chunhe on 2018-06-01.
 */
public abstract class AbstractMysqlReceiver4Redis extends MysqlReceiver {

    public AbstractMysqlReceiver4Redis() {
        super();
    }

    @Override
    protected void execute(Connection conn, final Event event) {
        if (event instanceof RedisMessaging) {
        } else {
            log.error(event.getClass().getName()+" is not RedisMessaging");
            return;
        }
        RedisMessaging redisMessaging = (RedisMessaging) event;
        String key = redisMessaging.key();
        String message = redisMessaging.message();
        execute(conn, key, message);
    }

    protected abstract void execute(Connection connection, String key, String message);

    private final static Log log = LogFactory.getLog(AbstractMysqlReceiver4Redis.class);
}