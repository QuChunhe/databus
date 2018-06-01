package databus.receiver.mysql;

import java.sql.Connection;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.event.redis.RedisMessaging;

public class MysqlReceiver4MultiKeys extends AbstractMysqlReceiver4Redis {
    
    public MysqlReceiver4MultiKeys() {
        super();
    }

    public void setMessageBeanMap(Map<String, MysqlBean> messageBeanMap) {
        this.messageBeanMap = messageBeanMap;
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

    }

    @Override
    protected void execute(Connection conn, String key, String message) {
        MysqlBean messageBean = messageBeanMap.get(key);
        if (null == messageBean) {
            log.error("Can not get value bean for "+key);
            return;
        }
        messageBean.execute(conn, key, message);
    }

    private final static Log log = LogFactory.getLog(MysqlReceiver4MultiKeys.class);

    private Map<String, MysqlBean> messageBeanMap;
}