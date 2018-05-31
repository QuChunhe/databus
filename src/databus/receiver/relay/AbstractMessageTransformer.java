package databus.receiver.relay;

/**
 * Created by Qu Chunhe on 2018-05-31.
 */
public abstract class AbstractMessageTransformer implements MessageTransformer {

    @Override
    public String topic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    private String topic;
}
