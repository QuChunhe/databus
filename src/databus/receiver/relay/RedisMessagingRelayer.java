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

    public void setMessageTransformerMap(Map<String, MessageTransformer> messageTransformerMap) {
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
        String oldKey = redisMessaging.key();
        MessageTransformer transformer = messageTransformerMap.get(oldKey);
        if (null == transformer) {
            log.error("Can not get Transformer for "+oldKey);
            return;
        }

        String newMessage = transformer.toMessage(redisMessaging.message());
        if (null == newMessage) {
            log.error("Can not transform message from : "+redisMessaging.message());
            return;
        }
        send(transformer.topic(), new RedisMessaging(redisMessaging.time(), oldKey, newMessage));
    }

    private final static Log log = LogFactory.getLog(RedisMessagingRelayer.class);

    private Map<String, MessageTransformer> messageTransformerMap;
}
