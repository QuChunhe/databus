package databus.receiver.kafka;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;

/**
 * Created by Qu Chunhe on 2018-05-24.
 */
public class KafakReceiver4Redis extends AbstractKafakReceiver4Redis {
    public KafakReceiver4Redis() {
        super();
    }

    public void setMessageTransformerMap(Map<String, MessageTransformer> messageTransformerMap) {
        this.messageTransformerMap = messageTransformerMap;
    }

    @Override
    protected void send(String redisKey, String redisMessage) {
        MessageTransformer transformer = messageTransformerMap.get(redisKey);
        if (null == transformer) {
            log.error("Can not get Transformer for "+redisKey);
            return;
        }
        String topic = transformer.topic(redisMessage);
        if (null == topic) {
            log.error("Can not get topic : "+redisMessage);
            return;
        }
        String key = transformer.key(redisMessage);
        if (null == key) {
            log.error("Can not get key : "+redisMessage);
            return;
        }
        String value = transformer.value(redisMessage);
        if (null == value) {
            log.error("Can not get value : "+redisMessage);
            return;
        }
        send(topic, key, value);
    }

    private Map<String, MessageTransformer> messageTransformerMap;
    private final static Log log = LogFactory.getLog(KafakReceiver4Redis.class);

}
