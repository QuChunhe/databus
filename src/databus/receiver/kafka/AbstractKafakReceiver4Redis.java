package databus.receiver.kafka;

import databus.core.Event;
import databus.event.redis.RedisMessaging;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by Qu Chunhe on 2018-05-24.
 */
public abstract class AbstractKafakReceiver4Redis extends KafkaRecevier {
    public AbstractKafakReceiver4Redis() {
        super();
    }

    @Override
    public void receive(Event event) {
        if (event instanceof RedisMessaging) {
        } else {
            log.error(event.getClass().getName()+" is not RedisMessaging");
            return;
        }
        RedisMessaging redisMessaging = (RedisMessaging) event;
        String redisKey = redisMessaging.key();
        String redisMessage = redisMessaging.message();
    }

    protected abstract void send(String redisKey, String redisMessage);

    private final static Log log = LogFactory.getLog(AbstractKafakReceiver4Redis.class);
}
