package databus.receiver.relay;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.event.redis.RedisMessaging;

/**
 * Created by Qu Chunhe on 2018-05-31.
 */
public class RedisMessagingRelayer extends KafkaRelayer {
    public RedisMessagingRelayer() {
        super();
    }

    public void setMessageTransformerMap(Map<String, EventTransformer> messageTransformerMap) {
        this.messageTransformerMap = messageTransformerMap;
    }

    @Override
    public void receive(Event event) {
        if (event instanceof RedisMessaging) {
        } else {
            log.error(event.getClass().getName()+" is not RedisMessaging");
            return;
        }
        RedisMessaging redisMessaging = (RedisMessaging) event;
        EventTransformer transformer = messageTransformerMap.get(redisMessaging.key());
        if (null == transformer) {
            log.error("Can not get Transformer for "+redisMessaging.key());
            return;
        }

        EventWrapper eventWrapper = transformer.transform(redisMessaging);
        if (null == eventWrapper) {
            log.error("Can not transform event from : "+redisMessaging.message());
            return;
        }
        send(eventWrapper.getTopic(), eventWrapper.getEvent());
    }

    private final static Log log = LogFactory.getLog(RedisMessagingRelayer.class);

    private Map<String, EventTransformer> messageTransformerMap;
}
