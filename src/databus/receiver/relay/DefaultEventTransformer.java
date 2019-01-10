package databus.receiver.relay;

import databus.core.Event;
import databus.network.DefaultKeyMapper;
import databus.network.KeyMapper;

/**
 * Created by Qu Chunhe on 2019-01-09.
 */
public class DefaultEventTransformer implements EventTransformer {

    public DefaultEventTransformer() {
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setKeyMapper(KeyMapper keyMapper) {
        this.keyMapper = keyMapper;
    }

    @Override
    public EventWrapper transform(Event event) {
        return new EventWrapper(topic, keyMapper.toKey(event), event);
    }

    protected KeyMapper keyMapper = new DefaultKeyMapper();

    private String topic;
}
