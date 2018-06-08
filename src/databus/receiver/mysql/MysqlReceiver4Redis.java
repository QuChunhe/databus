package databus.receiver.mysql;

import java.sql.Connection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.event.redis.RedisMessaging;

/**
 * Created by Qu Chunhe on 2018-06-01.
 */
public class MysqlReceiver4Redis extends MysqlReceiver {

    public MysqlReceiver4Redis() {
        super();
    }

    public void setMysqlBean(MessageBean mysqlBean) {
        this.mysqlBean = mysqlBean;
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
        mysqlBean.execute(conn, key, message);
    }


    private final static Log log = LogFactory.getLog(MysqlReceiver4Redis.class);

    private MessageBean mysqlBean;
}