package databus.receiver.relay;

import databus.core.Event;
/**
 * Created by Qu Chunhe on 2018-06-01.
 */
public class EventWrapper {
    public EventWrapper(String topic, Event event) {
        this.topic = topic;
        this.event = event;
    }

    public String getTopic() {
        return topic;
    }

    public Event getEvent() {
        return event;
    }

    private final String topic;
    private final Event event;

}
