package databus.receiver.kafka;

/**
 * Created by Qu Chunhe on 2018-05-24.
 */
public abstract class AbstractMessageTransformer implements MessageTransformer {

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setKey(String key) {
        this.key = key;
    }

    @Override
    public String topic(String redisMessage) {
        return topic;
    }

    @Override
    public String key(String redisMessage) {
        return key;
    }

    private String topic;
    private String key;
}
