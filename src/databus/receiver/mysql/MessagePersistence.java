package databus.receiver.mysql;

import java.sql.Connection;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.event.redis.RedisMessaging;

public class MessagePersistence extends MysqlReceiver {
    
    public MessagePersistence() {
        super();
    }

    public void setMessageBeanMap(Map<String, MessageBean> messageBeanMap) {
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
        MessageBean messageBean = messageBeanMap.get(key);
        if (null == messageBean) {
            log.error("Can not get message bean for "+key);
            return;
        }
        String message = redisMessaging.message();
        messageBean.execute(conn, key, message);
    }

    private final static Log log = LogFactory.getLog(MessagePersistence.class);

    private Map<String, MessageBean> messageBeanMap;
}