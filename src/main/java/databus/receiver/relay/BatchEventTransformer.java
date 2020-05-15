package databus.receiver.relay;

import databus.core.Event;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;

/**
 * Created by Qu Chunhe on 2018-06-08.
 */
public abstract class BatchEventTransformer implements EventTransformer {
    public BatchEventTransformer() {
    }

    public void setEventTransformerMap(Map<String, EventTransformer> eventTransformerMap) {
        this.eventTransformerMap = eventTransformerMap;
    }

    @Override
    public EventWrapper transform(Event event) {
        String key = toKey(event);
        if (null == key) {
            log.error("Can not get key for "+event.toString());
            return null;
        }
        EventTransformer eventTransformer = eventTransformerMap.get(key);
        if (null == eventTransformer) {
            return null;
        }
        return eventTransformer.transform(event);
    }

    protected abstract String toKey(Event event);

    private final static Log log = LogFactory.getLog(BatchEventTransformer.class);

    private Map<String, EventTransformer> eventTransformerMap;
}
