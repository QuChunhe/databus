package databus.receiver.cassandra;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.event.redis.RedisMessaging;


/**
 * Created by Qu Chunhe on 2018-06-04.
 */
public class RedisBatchCassandraBean extends BatchCassandraBean  {
    public RedisBatchCassandraBean() {
    }

    @Override
    protected String toKey(Event event) {
        if (event instanceof RedisMessaging) {
            RedisMessaging redisMessaging = (RedisMessaging) event;
            return redisMessaging.key();
        }
        log.error("Is not a RedisMessaging : "+event.toString());
        return null;
    }

    private final static Log log = LogFactory.getLog(RedisBatchCassandraBean.class);

}
