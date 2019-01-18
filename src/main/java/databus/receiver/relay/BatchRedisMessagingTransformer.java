package databus.receiver.relay;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.event.redis.RedisMessaging;

/**
 * Created by Qu Chunhe on 2018-05-31.
 */
public class BatchRedisMessagingTransformer extends BatchEventTransformer {
    public BatchRedisMessagingTransformer() {
        super();
    }

    @Override
    protected String toKey(Event event) {
        if (event instanceof RedisMessaging) {
        } else {
            log.error(event.getClass().getName()+" is not RedisMessaging");
            return null;
        }
        RedisMessaging redisMessaging = (RedisMessaging) event;
        return redisMessaging.key();
    }

    private final static Log log = LogFactory.getLog(BatchRedisMessagingTransformer.class);
}
