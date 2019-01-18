package databus.receiver.relay;

import databus.core.Event;
/**
 * Created by Qu Chunhe on 2018-06-01.
 */
public class EventWrapper {
    public EventWrapper(String topic, String key, Event event) {
        this.topic = topic;
        this.key = key;
        this.event = event;
    }

    public EventWrapper(String topic, Event event) {
        this(topic, null, event);
    }


    public String getTopic() {
        return topic;
    }

    public Event getEvent() {
        return event;
    }

    public String getKey() {
        return key;
    }

    private final String topic;
    private final Event event;
    private final String key;

}
